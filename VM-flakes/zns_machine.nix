{ config, lib, pkgs, ... }:
# let
#      pkgs = import (builtins.fetchGit {
#          # Descriptive name to make the store path easier to identify
#          name = "my-old-revision";
#          url = "https://github.com/NixOS/nixpkgs/";
#          ref = "refs/heads/nixos-22.05";
#          rev = "7d7622909a38a46415dd146ec046fdc0f3309f44";
#      }) {};

#      myPkg = pkgs.nvme-cli;
# in
{
  imports = [
    ./common
  ];

  # TODO every build makes a new disk image
  # Provide flake for sinkhole, build new image with new sinkhole binary
  boot.kernelPackages = pkgs.linuxPackages_5_15;
  boot.kernelPatches = [ {
    name = "zns-support";
    patch = null;
    extraConfig = ''
      BLK_DEV_ZONED y
      MQ_IOSCHED_DEADLINE y
      ZONEFS_FS y
      '';
  } ];

  # boot.kernelPackages = pkgs.linuxPackagesFor (pkgs.linux_5_19.override {
  #   argsOverride = rec {
  #     src = pkgs.fetchurl {
  #       url = "mirror://kernel/linux/kernel/v4.x/linux-${version}.tar.xz";
  #       sha256 = "0ibayrvrnw2lw7si78vdqnr20mm1d3z0g6a0ykndvgn5vdax5x9a";
  #     };
  #     version = "5.19.60";
  #     modDirVersion = "5.19.60";
  #   };
  # });
}
