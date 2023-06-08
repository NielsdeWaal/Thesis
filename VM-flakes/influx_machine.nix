{ config, lib, pkgs, ... }:
{
  imports = [
    ./common
    # <home-manager/nixos>
  ];

  boot.kernelPackages = pkgs.linuxPackages_6_1;

  services.influxdb = {
    enable = true;
  };

}
