#!/usr/bin/env bash

# Set up the worker service if app successfully built
if [ -f ./bin/application ]; then
  echo
  echo "Configuring the wordfreq service..."
  sudo systemctl stop wordfreq > /dev/null 2>&1
  sudo systemctl disable wordfreq > /dev/null 2>&1
  sudo useradd wordfreqservice -s /sbin/nologin -M > /dev/null 2>&1
  sudo cp wordfreq.service /lib/systemd/system/
  sudo chmod 755 /lib/systemd/system/wordfreq.service
  sudo systemctl enable wordfreq.service
  echo "Starting wordfreq service..."
  sudo systemctl start wordfreq.service
  sudo systemctl is-active -q wordfreq.service
  if [ $? -eq 0 ]; then
    echo "Service started successfully."
    echo
    echo "To see log output, type:"
    echo "sudo journalctl -f -u wordfreq"
    echo "(CTRL+C to exit log reader)"
  else
    echo "Service could not be started. If this persists, please re-run setup."
  fi
else
  echo
  echo "No file at bin/application - run setup first."
fi
echo
