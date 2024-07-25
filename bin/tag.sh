read -p "Enter tag message: " desc
read -p "Enter release version (e.g. 0.1.9): " VERSION
export version=`cat blazer/__version__.py |grep "__version__ ="|awk '{print $3}'|sed "s/\"//g"`; sed='s/'$version'/'$VERSION'/g'; perl -pi.bak -e $sed blazer/__version__.py; rm blazer/*bak
export version=`cat docs/source/conf.py |grep "release = "|awk '{print $3}'|sed s/\'//g`; sed='s/'$version'/'$VERSION'/g'; perl -pi.bak -e $sed docs/source/conf.py; rm docs/source/*bak
git add blazer docs

git tag -a "v$VERSION"  -m "$desc"
git commit -m "$desc"
git push origin main
git push origin "v$VERSION"
rm dist/*
python setup.py sdist
twine upload dist/*

