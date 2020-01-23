# Change Log
All notable changes to this docker image will be documented in this file.



## [v1.10.6-5] 

### Changed
* moved pip package list into requirements.txt


### Added
* added list of python dependencies to support TASC datamart deployment [DEVOPS-351]
* added R language support with related packages [DEVOPS-351]

### Fixed


## [v1.10.6-4] 

### Added
* add pip packages to support google oauth `oauthlib`,`flask-oauthlib`

### Fixed
* pin flask-appbuilder and marshmallow version for fix dep conflict

## [v1.10.6-2] 

### Added
* add pip package to support aws calls fro DF `boto3`

## [v1.10.6-0] 

### Changed
* change airflow version to 1.10.6

### Added
* add custom pip package to metrics to Prometheus `github.com/snapcart/airflow-exporter.git`

[DEVOPS-351]: https://snapcart.atlassian.net/browse/DEVOPS-351