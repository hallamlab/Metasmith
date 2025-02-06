HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $HERE
pyinstaller relay.py --onefile --bootloader-ignore-signals 
