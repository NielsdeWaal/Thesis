{ stdenv, fetchurl }:
# with import <nixpkgs> {};

stdenv.mkDerivation {
  pname = "questdb";
  version = "7.1.3";

  src = fetchurl { 
    url = "https://github.com/questdb/questdb/releases/download/7.1.3/questdb-7.1.3-rt-linux-amd64.tar.gz"; 
    sha256 = "0gb2m8yz21c9qzzkx11b7pc7bh0j77d9pajq7900q7n21nfqaxcl"; 
  };
}
