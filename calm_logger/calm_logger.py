import os
import sys
import logging
import logging.config


current_dir_path = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)

sh = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
logger.addHandler(sh)
