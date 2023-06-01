{
  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    nixos-generators = {
      url = "github:nix-community/nixos-generators";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    home-manager.url = "github:nix-community/home-manager";
  };
  outputs = { self, nixpkgs, nixos-generators, home-manager, ... }: {
    packages.x86_64-linux = {
      zns_machine = nixos-generators.nixosGenerate {
        system = "x86_64-linux";
        modules = [
          # you can include your own nixos configuration here, i.e.
          ./zns_machine.nix
        ];
        format = "qcow";
      };
      influx_machine = nixos-generators.nixosGenerate {
        system = "x86_64-linux";
        modules = [
          ./influx_machine.nix
        ];
        format = "qcow";
      };
      questdb_machine = nixos-generators.nixosGenerate {
        system = "x86_64-linux";
        modules = [
          ./questdb_machine.nix
        ];
        format = "qcow";
      };
    };
  };
}
