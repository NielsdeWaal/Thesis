{ config, lib, pkgs, ... }:
{
  imports = [
    ./common
    # <home-manager/nixos>
  ];

  boot.kernelPackages = pkgs.linuxPackages_6_1;

  services.clickhouse = {
    enable = true;
  };

  environment.etc = {
    tsbs.source = ./tsbs;
  };
}
