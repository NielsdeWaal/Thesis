{ config, lib, pkgs, ... }:

{
  services.openssh.enable = true;

  users.users.deploy = {
    isNormalUser = true;
    extraGroups = [
      "wheel"
      "docker"
      "audio"
      "plugdev"
      "libvirtd"
      "adbusers"
      "dialout"
      "within"
    ];
    shell = pkgs.zsh;
    openssh.authorizedKeys.keys = [
      "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQD2Y5fMFzLwoV97vyk1t5NONHPViOonz+N4nHwylF61Ias/kQuBXqONkZjuPl5Vksy05bIujAd1fDkvOoLLCqSqxT/oiSISU+oYANV0P730nq+im919/lhdwmJ6kktyWplVac6GOr1Vrv6OeqVAF9L8kRASLlxteekqF2Vfpt/YYHPkvlp6DL/k2/AARz+HwdIc35aD2DCo/sENK8us1l8ZEE0UvmH0vHizWbkrGJnEv623AwKwbaA9VcgI5QlAi34xv7clRBQnC/h2RUsL+vfub4q313bS8C7YKq0VkGSCnyoKxKnKUs4+59Rq9e0h1vB7wLxpuOyw3jc6gvSmCz/YKlwHRASBgy4+0CDAYShy3N/aMiOqyxqYNCObHKGZjgAqbNONf7WOWMvwL6YfslhE7Wsg27JGCyfSrILXr/aQ3xmr37X9ICgZNDW3w0Tc2p/DbCKN6tcMQo2tN5FK+LuDImzqK0UpYvysO2Y1cfFCoGOjys6EzXMmpGhSaECJ0QIOtrWBkiU45zALzG4vXPNbtdbDM7t6bEpSK9u9fjABrgj41PG9Jy/rDzgZe/C5tQzIcCwgWqcUUdDOVFWEuZieVsQx4bG8L4e8T7igMQC9cP5LSG8TBJhx/cE25fF5QFVowbC02ycKOb/G34aBv0e029lsguAAeSgQ4IaiTqh6Dw== cardno:6167726"
    ];
  };

  users.users.root.openssh.authorizedKeys.keys =
    config.users.users.deploy.openssh.authorizedKeys.keys;

  environment.systemPackages = with pkgs;
    [
      vim 
      
      libuuid
      libnvme
      json_c
      zlib

      python311
      python311Packages.pyperf

      git
      nvme-cli
      # myPkg
      pciutils
      gcc
      gnumake
      fio

      go
    ];

  programs.zsh = {
    enable = true;
    enableCompletion = true;
    shellAliases = { l = "ls -la"; };

    autosuggestions.enable = true;
    syntaxHighlighting.enable = true;
    # autosuggestions = {
    #   enable = true;
    # };

    ohMyZsh = {
      enable = true;
      plugins = [ "git" "copypath" ];
    };
  };

  security.sudo.extraRules= [
    {  users = [ "deploy" ];
       commands = [
         { command = "ALL" ;
           options= [ "NOPASSWD" ]; # "SETENV" # Adding the following could be a good idea
         }
       ];
    }
  ];

  system.stateVersion = "23.05";
}
