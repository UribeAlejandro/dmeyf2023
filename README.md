# DM & KD Economía y Finanzas

## Setup

### Pre-requisites

- Install [Homebrew](https://brew.sh/).
- Install [Git](https://git-scm.com/).
- Install [Visual Studio Code](https://code.visualstudio.com/).

Finally, install [XCode](https://developer.apple.com/xcode/) and the command line tools:

```zsh
xcode-select --install
```

### Python Setup

Install [Miniconda](https://docs.conda.io/en/latest/miniconda.html) and create a new environment:

```zsh
conda create -n dm-kd python=3.8
```

Activate the environment:

```zsh
conda activate dm-kd
```

Install the required packages:

```zsh
pip install -r requirements.txt &&
pip install --upgrade jupyterlab jupyterlab-git
```

### R setup

Install [R](https://cran.r-project.org/) and [XQuartz](https://www.xquartz.org/).

Start a R session:

```zsh
R
```

Run the following commands:
```r
install.packages(c("repr", "IRdisplay", "evaluate", "crayon"))
install.packages(c('pbdZMQ', 'devtools', 'uuid', 'digest'), dependencies=TRUE)
install.packages('languageserver')
library('devtools')
devtools::install_github("ManuelHentschel/vscDebugger")
install.packages('IRkernel')
```

Finish the R session:

```r
quit()
```

Install the kernel for Jupyter:

```zsh
conda install -c r r-irkernel
```

### Julia setup

Install [Julia](https://julialang.org/downloads/) and [XQuartz](https://www.xquartz.org/).

```zsh
brew install julia
```

Start a Julia session:
```zsh
julia
```

Run the following commands to install the kernel for Jupyter:

```julia
using Pkg
Pkg.add("IJulia")
```

### GCloud setup

Install [GCloud](https://cloud.google.com/sdk/docs/install).

```zsh
brew install --cask google-cloud-sdk
```

Initialize GCloud:

```zsh
gcloud init
```

## Post Setup

### Pre-commit

Install the pre-commit hooks:

```zsh
pre-commit install
```
