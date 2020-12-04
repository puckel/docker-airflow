# Useful docker commands

## Docker up

Initially and after code changes:

```
docker-compose -f docker-compose-LocalExecutor.yml  up -d
```

## Docker down

To reset the databases: 

the connections need to be created again after a `docker down`.

``` 
docker-compose -f docker-compose-LocalExecutor.yml down
```
