from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as ver

import click

from ai.chronon.cli.compile.display.console import console
from ai.chronon.repo.compilev3 import compile_v3
from ai.chronon.repo.init import main as init_main
from ai.chronon.repo.run import main as run_main

LOGO = """
     =%%%@:-%%%@=:%%%@+     .%@%@@@@@@%%%%%%:                            -%%%=  
    :@@@@#.@@@@%.%@@@@.     .@@@@@@@@@@@@@@-   @@                        =@@@+   @@
   :@@@@*.%@@@#.#@@@%.              .#@@@@:    @@@                       =@@@+   @@@       
  =@@@@=-@@@@+:%@@@#.               #@@@%.            .%%=:+#%@@@%#+-    =@@@+         *%=         #%%*     :=#%@@@@#*-
.#@@@#-+@@@%-=@@@@-               .%@@@%.      @@@@  .@@@@@@@@%%@@@@%=   =@@@+  +@@@=  *@@@+.      %@@%   :#@@@@%%%@@@@@=
+**+=-%@@@+-#@@@*----=.          :@@@@#        %@@@  .@@@@%=.   .-#@@@*  =@@@+  +@@@=  *@@@@@*:    %@@%  -@@@%-     .+@@@*
    +@@@%-+@@@%-=@@@@+          :@@@@*         @@@@  .@@@@.        #@@@: =@@@+  +@@@=  *@@@%@@@*:  %@@%  %@@@#++****+*@@@@-
  -@@@@+:#@@@*:#@@@#.          -@@@@*          @@@@  .@@@@         *@@@- =@@@+  +@@@=  *@@@.-%@@@#-%@@%  @@@@****#****++++:
 =@@@@--@@@@=:@@@@*           =@@@@+           @@@@  .@@@@#.     .+@@@%  =@@@+  +@@@=  *@@@   -#@@@@@@%  =@@@*.      
+@@@@--@@@@=:@@@@*           +@@@@@#########+  @@@@  .@@@@@@%*+*#@@@@*   =@@@+  +@@@=  *@@@.    :#@@@@%   =@@@@%     -==+-
:@@@@* @@@@# @@@@%           *@@@@@@@@@@@@@@@%  @@@@  .@@@@#@@@@@@@%+:    =@@@+  +@@@=  *@@@.      :*@@%    .=#@@@@@@@%*:
                                                      .@@@%  
                                                      .@@@%
                                                      .@@@@
                                                       ---:
"""


def _set_package_version():
    try:
        package_version = ver("zipline-ai")
    except PackageNotFoundError:
        print("No package found. Continuing with the latest version.")
        package_version = "latest"
    return package_version


@click.group(help="The Zipline CLI. A tool for authoring and running Zipline pipelines in the cloud. For more information, see: https://chronon.ai/")
@click.version_option(version=_set_package_version())
@click.pass_context
def zipline(ctx):
    ctx.ensure_object(dict)
    ctx.obj["version"] = _set_package_version()


zipline.add_command(compile_v3)
zipline.add_command(run_main)
zipline.add_command(init_main)
