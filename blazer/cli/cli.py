import sys
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(filename)s: '    
                            '%(levelname)s: '
                            '%(funcName)s(): '
                            '%(lineno)d:\t'
                            '%(message)s')


logger = logging.getLogger(__name__)
import click

@click.group(invoke_without_command=True)
@click.option("--debug", is_flag=True, default=False, help="Debug switch")
@click.pass_context
def cli(context, debug):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    if debug:
        logging.basicConfig(
            format="%(asctime)s : %(name)s %(levelname)s : %(message)s", level=logging.DEBUG
        )
    else:
        logging.basicConfig(
            format="%(asctime)s : %(name)s %(levelname)s : %(message)s", level=logging.INFO
        )

    logger.info("Blazer CLI!")
    logger.debug("Blazer DEBUG!")


    if len(sys.argv) == 1:
        click.echo(context.get_help())