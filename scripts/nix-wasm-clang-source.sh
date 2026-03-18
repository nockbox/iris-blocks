export CC_wasm32_unknown_unknown=$(cat $(which clang) | grep clang | grep compiler=/nix | sed 's/.*compiler=\(.*\)$/\1/g')
