HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

PYTHONPATH=$HERE/..:$PYTHONPATH
python -m relay --io $HERE/cache
