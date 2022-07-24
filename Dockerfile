FROM lsstsqre/newinstall:latest
USER lsst
RUN source loadLSST.bash \
  && pip install redis
RUN source loadLSST.bash \
  && week=$(( $(date +%W) + 1 )) \
  && eups distrib install -t "w_$(date +%Y)_$week" obs_lsst
