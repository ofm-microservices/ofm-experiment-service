FROM grafana/k6:latest
COPY k6/full-system.js /work/full-system.js
COPY k6/resilience.js /work/resilience.js
COPY k6/formdata.js /work/formdata.js
ENTRYPOINT ["k6"]
