echo $TRAVIS_PULL_REQUEST $TRAVIS_BRANCH

if [[ "$TRAVIS_PULL_REQUEST" != "false" ]]; then
    echo "This is a pull request. No deployment will be done."; exit 0
fi


if [[ "$TRAVIS_BRANCH" != "master" ]]; then
    echo "No deployment on BRANCH='$TRAVIS_BRANCH'"; exit 0
fi


if [[ "2.7 3.4" =~ "$python" ]]; then
    anaconda -t "$BINSTAR_TOKEN"  upload --force --user iModels --package metamds-dev $HOME/miniconda/conda-bld/linux-64/metamds-*
    conda convert $HOME/miniconda/conda-bld/linux-64/metamds-* -p all
    ls
    anaconda -t "$BINSTAR_TOKEN"  upload --force --user iModels --package metamds-dev linux-32/metamds-*
    anaconda -t "$BINSTAR_TOKEN"  upload --force --user iModels --package metamds-dev win-32/metamds-*
    anaconda -t "$BINSTAR_TOKEN"  upload --force --user iModels --package metamds-dev win-64/metamds-*
    anaconda -t "$BINSTAR_TOKEN"  upload --force --user iModels --package metamds-dev osx-64/metamds-*
fi

if [[ "$python" != "2.7" ]]; then
    echo "No deploy on PYTHON_VERSION=${python}"; exit 0
fi
