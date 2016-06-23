# Installation

## Ubuntu
```shell
# system requirements
sudo apt-add-repository -y ppa:ubuntugis/ubuntugis-unstable
sudo apt-get update
sudo apt-get install -y libgdal-dev git python-pip python-dev libfreetype6-dev

# python requirements
sudo pip install --upgrade pip
sudo pip install numpy scipy matplotlib
sudo pip install gdal==1.11.2 --global-option=build_ext --global-option="-I/usr/include/gdal/"

# tilematrix & mapchete master
# alternatively, to install the latest release, use
# "pip install mapchete"
git clone https://github.com/ungarj/tilematrix.git
cd tilematrix
sudo python setup.py install
cd ..
git clone https://github.com/ungarj/mapchete.git
cd mapchete
sudo python setup.py install
cd ..
```
