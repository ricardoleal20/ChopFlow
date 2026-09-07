#!/usr/bin/env python3
"""Generate Python gRPC stubs from the ChopFlow proto.

Run after changing `broker/proto/chopflow.proto`:

    python generate.py

Writes `chopflow_pb2.py` and `chopflow_pb2_grpc.py` into `src/chopflow/_generated/`.
The generated files are committed so an install never needs `protoc`.
"""

from __future__ import annotations

import sys
from pathlib import Path

from grpc_tools import protoc


def main() -> int:
    here = Path(__file__).resolve().parent
    repo_root = here.parent.parent
    proto_dir = repo_root / "broker" / "proto"
    proto = proto_dir / "chopflow.proto"
    out_dir = here / "src" / "chopflow" / "_generated"
    out_dir.mkdir(parents=True, exist_ok=True)

    # grpc_tools ships its own well-known protos (google/protobuf/*.proto);
    # point -I at the grpc_tools include dir so timestamp.proto/empty.proto resolve.
    import grpc_tools

    grpc_tools_include = Path(grpc_tools.__file__).parent / "_proto"

    args = [
        "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"-I{grpc_tools_include}",
        f"--python_out={out_dir}",
        f"--grpc_python_out={out_dir}",
        str(proto),
    ]
    rc = protoc.main(args)
    if rc != 0:
        print("protoc failed", file=sys.stderr)
        return rc

    # The generated modules import each other as `import chopflow_pb2`, which
    # only resolves if `_generated` is on sys.path. Rewrite the grpc stub's
    # internal import to a package-relative one so it works as a subpackage.
    grpc_stub = out_dir / "chopflow_pb2_grpc.py"
    text = grpc_stub.read_text()
    text = text.replace(
        "import chopflow_pb2", "from chopflow._generated import chopflow_pb2"
    )
    grpc_stub.write_text(text)

    init = out_dir / "__init__.py"
    init.write_text(
        '"""Generated ChopFlow gRPC stubs. Regenerate via `python generate.py`.\n"""\n'
    )

    print(f"Generated stubs in {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
