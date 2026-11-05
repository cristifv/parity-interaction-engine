//! DEMO tool (step 3): TC's view of one session, read with `GetSession`, printed as one JSON line.
//! Used by `demo/run.sh` after `kill -9` of IE, to show the session still live on TC with its
//! owner's replica gone. Read-only; the test-peer header, as IE sends it before mTLS (TC step 8).
//!
//! `cargo run -p ie-telephony-client --example tc_session -- <tc-grpc-uri> <bin> <replica_id> <tenant_id> <interaction_id>`

use parity_proto::telephony::v1 as pb;
use pb::telephony_service_client::TelephonyServiceClient;
use std::time::Duration;
use tonic::metadata::AsciiMetadataValue;

#[allow(clippy::print_stdout, clippy::print_stderr)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [uri, bin, replica, tenant, interaction] = args.as_slice() else {
        eprintln!(
            "usage: tc_session <tc-grpc-uri> <bin> <replica_id> <tenant_id> <interaction_id>"
        );
        return std::process::ExitCode::from(2);
    };
    let channel = match tonic::transport::Endpoint::from_shared(uri.clone()) {
        Ok(e) => e.connect_timeout(Duration::from_secs(2)).connect_lazy(),
        Err(e) => {
            eprintln!("bad uri: {e}");
            return std::process::ExitCode::from(2);
        }
    };
    let mut client = TelephonyServiceClient::new(channel);
    let mut req = tonic::Request::new(pb::GetSessionRequest {
        tenant_id: tenant.clone(),
        interaction_id: interaction.clone(),
        replica_id: replica.clone(),
    });
    req.set_timeout(Duration::from_secs(2));
    for (k, v) in [
        ("tenant-id", tenant.clone()),
        (
            "x-parity-test-peer",
            format!("parity-interaction-engine.{bin}"),
        ),
    ] {
        if let Ok(v) = AsciiMetadataValue::try_from(v.as_str()) {
            req.metadata_mut().insert(k, v);
        }
    }
    match client.get_session(req).await {
        Ok(r) => {
            let r = r.into_inner();
            let Some(s) = r.session else {
                eprintln!("no session in the answer");
                return std::process::ExitCode::from(1);
            };
            let legs: Vec<String> = s
                .legs
                .iter()
                .map(|l| {
                    format!(
                        r#"{{"leg_id":"{}","role":"{:?}","state":"{:?}","play_id":"{}","answered_at_ms":{}}}"#,
                        l.leg_id,
                        l.role(),
                        l.state(),
                        l.play_id,
                        l.answered_at_ms
                    )
                })
                .collect();
            println!(
                r#"{{"state":"{:?}","owner_epoch":{},"owner_replica_id":"{}","media":"{:?}","last_delivery_seq":{},"end_reason":"{:?}","incarnation":"{}","legs":[{}]}}"#,
                s.state(),
                s.owner_epoch,
                s.owner_replica_id,
                s.media(),
                s.last_delivery_seq,
                s.end_reason(),
                r.incarnation,
                legs.join(",")
            );
            std::process::ExitCode::SUCCESS
        }
        Err(e) => {
            let code = e
                .metadata()
                .get("parity-error")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            eprintln!("GetSession failed: {:?} {code}", e.code());
            std::process::ExitCode::from(1)
        }
    }
}
