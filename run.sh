#! /bin/sh
#----------------------------------------------------------------

HOME_DIR="/home/parser3/parser-27"

cd ${HOME_DIR}
# virtualenv -p `which python3` .venv3

. ${HOME_DIR}/.venv3/bin/activate

# pip install -r requirements.txt
# pip freeze

./parser_27_prod.py -c parser.conf

deactivate
