#!/bin/bash
# testpp を、インストールせず in-place の lib をリンクして実行する。
set -e
cd "$(dirname "$0")"
export LD_LIBRARY_PATH="../coseq:../coseqpp:${LD_LIBRARY_PATH}"
./testpp
