//! IE-023: the STEP-3 MINIMAL lease against a real etcd (a container on a random loopback port,
//! removed at the end), reached through a TCP proxy the test can cut. A cut shorter than the TTL
//! keeps the lease; a cut longer than the TTL (10 s) loses it (no lease: the call path declines
//! and readiness is pending), and once etcd is back a new lease is granted with the replica key
//! and claims work again.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use interaction_engine::calls::owner::{
    self, Claim, Etcd, Lease, OwnerRecord, Registration, replica_key,
};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::AbortHandle;

/// etcd v3.7.2, pinned by digest (the image TC's tests and the demo run).
const IMAGE: &str = "gcr.io/etcd-development/etcd@sha256:7c6c239825d00e3f6328a69caafd54be92063acf0c2ce78b8394699f52b75dc3";
const TTL_S: i64 = 10;

struct Container(String);

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
    }
}

fn docker(args: &[&str]) -> String {
    let out = Command::new("docker").args(args).output().expect("docker");
    assert!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

fn etcd_container() -> (Container, u16) {
    let id = docker(&[
        "run",
        "-d",
        "--rm",
        "--label",
        "parity.test=parity-interaction-engine",
        "--security-opt",
        "label=disable",
        "--cap-drop",
        "ALL",
        "--tmpfs",
        "/tmp:mode=1777",
        "-p",
        "127.0.0.1::2379",
        IMAGE,
        "etcd",
        "--name",
        "t",
        "--data-dir",
        "/tmp/etcd",
        "--listen-client-urls",
        "http://0.0.0.0:2379",
        "--advertise-client-urls",
        "http://127.0.0.1:2379",
    ]);
    let c = Container(id);
    let port: u16 = docker(&["port", &c.0, "2379/tcp"])
        .lines()
        .next()
        .and_then(|l| l.rsplit(':').next())
        .and_then(|p| p.parse().ok())
        .expect("etcd's port");
    (c, port)
}

/// A TCP proxy to etcd that can be cut: a cut closes every connection and refuses new ones
/// (etcd keeps running, so it expires the lease on its own clock: a partition, not a restart).
#[derive(Clone)]
struct Proxy {
    up: Arc<Mutex<bool>>,
    conns: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Proxy {
    async fn start(upstream: u16) -> (Self, u16) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let p = Self {
            up: Arc::new(Mutex::new(true)),
            conns: Arc::default(),
        };
        let me = p.clone();
        tokio::spawn(async move {
            while let Ok((mut down, _)) = l.accept().await {
                if !*me.up.lock().unwrap() {
                    drop(down);
                    continue;
                }
                let h = tokio::spawn(async move {
                    if let Ok(mut up) = TcpStream::connect(("127.0.0.1", upstream)).await {
                        let _ = tokio::io::copy_bidirectional(&mut down, &mut up).await;
                    }
                });
                me.conns.lock().unwrap().push(h.abort_handle());
            }
        });
        (p, port)
    }

    fn cut(&self) {
        *self.up.lock().unwrap() = false;
        for h in self.conns.lock().unwrap().drain(..) {
            h.abort();
        }
    }

    fn restore(&self) {
        *self.up.lock().unwrap() = true;
    }
}

fn record(replica: &str) -> OwnerRecord {
    OwnerRecord {
        tenant_id: "acme".into(),
        bin: "bin-a".into(),
        replica_id: replica.into(),
        node_id: "media-1".into(),
    }
}

async fn healthy(etcd: &Etcd) {
    let end = Instant::now() + Duration::from_secs(30);
    while etcd.get("health-probe").await.is_err() {
        assert!(Instant::now() < end, "etcd never answered");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Holds `f` for `for_ms`, sampling every 50 ms.
async fn stays(what: &str, for_ms: u64, f: impl Fn() -> bool) {
    let end = Instant::now() + Duration::from_millis(for_ms);
    while Instant::now() < end {
        assert!(f(), "{what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn within(what: &str, ms: u64, f: impl Fn() -> bool) -> Duration {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < Duration::from_millis(ms),
            "not within {ms} ms: {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    start.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lease_lost_in_an_etcd_outage_longer_than_its_ttl_is_granted_again() {
    ie_obs::testing::capture_to_file("etcd_lease");
    let (_c, etcd_port) = etcd_container();
    let (proxy, port) = Proxy::start(etcd_port).await;
    let etcd = Etcd::connect(
        &format!("http://127.0.0.1:{port}"),
        Duration::from_millis(500),
    )
    .unwrap();
    let direct = Etcd::connect(
        &format!("http://127.0.0.1:{etcd_port}"),
        Duration::from_millis(500),
    )
    .unwrap();
    healthy(&direct).await;
    let replica = "ie-bin-a-lease-test";
    let reg = Registration {
        bin: "bin-a".into(),
        replica_id: replica.into(),
        value: br#"{"replica_id":"ie-bin-a-lease-test","bin":"bin-a","nodes":["media-1"]}"#
            .to_vec(),
        ttl_s: TTL_S,
    };
    let (_stop_tx, mut stop) = tokio::sync::watch::channel(false);
    let first = owner::register(&etcd, &reg, |_| {}, &mut stop)
        .await
        .unwrap();
    let lease = Lease::new(first);
    tokio::spawn(owner::maintain(
        etcd.clone(),
        reg.clone(),
        lease.clone(),
        stop.clone(),
    ));
    let iid1 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819203";
    assert!(matches!(
        etcd.claim(iid1, &record(replica), first).await.unwrap(),
        Claim::Claimed(_)
    ));

    // A blip shorter than the TTL: the lease survives, readiness never drops.
    proxy.cut();
    let l = lease.clone();
    stays("the lease is kept through a 4 s blip", 4_000, || {
        l.current() == Some(first)
    })
    .await;
    proxy.restore();
    stays("and after it", 4_000, || l.current() == Some(first)).await;

    // An outage longer than the TTL: etcd drops the lease and its keys; IE has no lease.
    proxy.cut();
    let lost_after = within("the lease presumed lost", 12_000, || l.current().is_none()).await;
    tokio::time::sleep(Duration::from_millis(12_500).saturating_sub(lost_after)).await;
    assert_eq!(l.current(), None, "no lease while etcd is unreachable");
    assert_eq!(
        direct.get(&replica_key("bin-a", replica)).await.unwrap(),
        None,
        "etcd expired the old lease and its replica key"
    );
    assert_eq!(direct.get(&owner::owner_key(iid1)).await.unwrap(), None);

    // etcd is back: a new lease, the replica key on it, claims resume.
    proxy.restore();
    let back_after = within("a new lease", 8_000, || l.current().is_some()).await;
    let second = l.current().unwrap();
    assert_ne!(second, first);
    assert_eq!(l.regrants(), 1);
    let key = direct
        .get(&replica_key("bin-a", replica))
        .await
        .unwrap()
        .expect("the replica key again");
    assert_eq!(key.lease, second);
    assert_eq!(key.value, reg.value);
    let iid2 = "0192f6a1-7c3e-7b21-9a4d-5e6f70819204";
    assert!(matches!(
        etcd.claim(iid2, &record(replica), second).await.unwrap(),
        Claim::Claimed(_)
    ));
    // A claim written on the lost lease is refused as such (the call path then marks it lost).
    assert_eq!(
        etcd.claim(
            "0192f6a1-7c3e-7b21-9a4d-5e6f70819205",
            &record(replica),
            first
        )
        .await,
        Err(owner::EtcdError::LeaseNotFound)
    );
    println!(
        "IE-023: lease {first:x} presumed lost {:.1} s into a 12.5 s outage; lease {second:x} live {:.1} s after etcd returned",
        lost_after.as_secs_f64(),
        back_after.as_secs_f64()
    );
}
