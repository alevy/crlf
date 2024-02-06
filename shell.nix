{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.rustup

    # keep this line if you use bash
    pkgs.bashInteractive
  ];
}
