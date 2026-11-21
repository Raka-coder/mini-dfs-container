# Architectural Decisions Record (ADR)

## Decision 1: Podman Execution Fallback via scripts/cluster.py
- **Context:** Podman 6.0.2 Windows remote client encountered socket forwarding issues over local Windows named pipes / ports to WSL2 (`podman-machine-default`).
- **Decision:** As specified in `IMPLEMENTATION_GUIDE.md` Section A6 and B8.4, we implement and utilize `scripts/cluster.py` which interfaces directly with `podman` inside the WSL2 machine (`wsl -d podman-machine-default -u root podman ...`). This guarantees consistent rootless container volume operations, exact CPU/memory constraints, and multi-node network isolation across any Windows host setup without requiring PowerShell or shell scripts.

## Decision 2: Pure Python Stack
- Python 3.12-slim is used as the base container environment. gRPC stub compilation is integrated into the multi-stage build.
