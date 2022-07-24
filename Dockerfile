FROM lsstsqre/newinstall:latest
USER lsst
RUN source loadLSST.bash \
  && pip install redis
RUN source loadLSST.bash \
  && eups distrib install -t w_latest obs_lsst
