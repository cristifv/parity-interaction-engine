# The spike's tools image (ADR 0002 §2, S9): SIPp, tcpdump, ss and the tone script's python,
# on the same Fedora as the host, every part pinned.
FROM docker.io/library/fedora@sha256:a651ddf48ea28a06ed4e1e6519f51c9f47e7a5a138722ade87369b8fbb7e5b42
RUN dnf -y --setopt=install_weak_deps=False install \
      sipp-3.8.0-1.fc43 tcpdump-14:4.99.6-2.fc43 iproute-6.14.0-2.fc43 python3-3.14.8-1.fc43 \
    && dnf clean all
