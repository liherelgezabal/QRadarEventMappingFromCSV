# run as: ./getCert.sh qradar_host:port
echo | openssl s_client -connect $1  2>/dev/null | openssl x509 
