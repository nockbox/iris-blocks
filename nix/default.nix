{ stdenv, pkgs, lib, craneLib, rustToolchainFor, ... }:
let
  rustToolchain = rustToolchainFor pkgs;

  root = ../.;
  src = lib.fileset.toSource {
    inherit root;
    fileset = lib.fileset.unions [
      # Default files from crane (Rust and cargo files)
      (craneLib.fileset.commonCargoSources root)
      # Also embed database migrations
      (lib.fileset.fileFilter (file: file.hasExt "sql") root)
    ];
  };
  commonArgs = {
    inherit src;
    version = "0.0.1";
    pname = "iris-blocks-deps";
    strictDeps = true;
    doCheck = false;
    nativeBuildInputs = [ pkgs.protobuf_29 ];
  };

  package-base = psrc: profile: extraArgs: craneLib.buildPackage (
  commonArgs // {
    inherit (craneLib.crateNameFromCargoToml { src = psrc; }) pname version;
    CARGO_PROFILE = profile;
    cargoExtraArgs = "${extraArgs}";
  });

  iris-blocks = extraArgs: (package-base ./.. "release" "--features=binary --bin iris-blocks ${extraArgs}");
in
{
  iris-blocks = iris-blocks "";
}
