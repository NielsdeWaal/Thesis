{ config, lib, pkgs, stdenv, fetchurl, ... }:
let 
  questdb = stdenv.mkDerivation {
    pname = "questdb";
    version = "7.1.3";

    src = fetchurl { 
      url = "https://github.com/questdb/questdb/releases/download/7.1.3/questdb-7.1.3-rt-linux-amd64.tar.gz"; 
      sha256 = "0gb2m8yz21c9qzzkx11b7pc7bh0j77d9pajq7900q7n21nfqaxcl"; 
    };
  };
in
{
  imports = [
    ./common
    # <home-manager/nixos>
  ];

  boot.kernelPackages = pkgs.linuxPackages_6_1;

  services.influxdb = {
    enable = true;
  };

  environment.etc = {
    tsbs.source = ./tsbs;
  };

  virtualisation.docker = {
    enable = true;
    enableOnBoot = true;
  };
  virtualisation.oci-containers = {
    backend = "docker";
    containers = {
      questdb = {
        image = "questdb/questdb:7.1.3";
        ports = ["9000:9000" "9009:9009" "8812:8812" "9003:9003"];
        volumes = ["/home/deploy/questdb:/var/lib/questdb"];
      };
    };
  };
}
