#!/bin/bash -e

case "$1" in
  webserver|flower|version|worker|scheduler)
    CMD="$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Turn it into
    # a file to avoid shell quoting nightmares.
    CUSTOM=$(mktemp)
    echo "#!/bin/bash -ex" > ${CUSTOM}
    chmod 755 ${CUSTOM}
    echo "$@" >> ${CUSTOM}
    CMD="${CUSTOM}"
    echo "*** Custom command:"
    echo
    cat ${CUSTOM}
    echo
    echo "*** End custom command"
    ;;
esac

echo "*** CMD: '${CMD}'"

if [ "${WATCH_FILES}" ]; then
  WATCHED_FILES=""
  for file in ${WATCH_FILES}; do
    if ! [ -e "${file}" ]; then
      echo "*** WARNING: ${file} does not appear to exist; attempting to create it"
      touch "${file}"
    fi
  done
  echo "*** Watching: ${WATCH_FILES}"
  ls ${WATCH_FILES} | entr -r /run.sh ${CMD}
fi

echo "*** Watching no files"
exec /run.sh ${CMD}
