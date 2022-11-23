# Basic saspy configuration template for a stand alone install.
# For full details on configuration for your own setup
# see the configuration doc at https://sassoftware.github.io/saspy/install.html#configuration


SAS_config_names = ["default"]

SAS_config_options = {"lock_down": False, "verbose": True, "prompt": True}

SAS_output_options = {
    "output": "html5",  # not required unless changing any of the default
    "style": "HTMLBlue",
}


default = {"saspath": "/opt/sasinside/SASHome/SASFoundation/9.4/bin/sas_u8"}
