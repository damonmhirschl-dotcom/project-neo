#!/bin/bash
set -euo pipefail
AWS_REGION="eu-west-2"
S3_BUCKET="algodesk-agent-deployments-dev"
S3_KEY="agents/Execution_Agent/execution_agent.py"
EC2_TARGET_DIR="/root/Project_Neo_Damon/Execution_Agent"
SYSTEMD_SERVICE="neo-execution-agent"
VENV_PATH="/root/algodesk/algodesk"
LOG_DIR="/var/log/neo"
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
echo "=================================================="
echo " Project Neo — Execution Agent Deployment"
echo "=================================================="
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; AGENT_FILE="${SCRIPT_DIR}/execution_agent.py"
[[ ! -f "${AGENT_FILE}" ]] && echo -e "${RED}ERROR: execution_agent.py not found${NC}" && exit 1
echo -e "${GREEN}✅ Step 1: execution_agent.py found ($(wc -l < "${AGENT_FILE}") lines)${NC}"
echo -e "${YELLOW}Running tests...${NC}"; source "${VENV_PATH}/bin/activate" 2>/dev/null || true
python "${AGENT_FILE}" --test || { echo -e "${RED}❌ Tests FAILED${NC}"; exit 1; }
echo -e "${GREEN}✅ Step 2: Tests passed${NC}"
aws s3 cp "${AGENT_FILE}" "s3://${S3_BUCKET}/${S3_KEY}" --region "${AWS_REGION}"
echo -e "${GREEN}✅ Step 3: Uploaded to S3${NC}"
mkdir -p "${EC2_TARGET_DIR}" "${LOG_DIR}"
if [ "$(realpath "${AGENT_FILE}")" != "$(realpath "${EC2_TARGET_DIR}/execution_agent.py" 2>/dev/null)" ]; then
    # Safety check: abort if source is SMALLER than production (prevents dev regression)
    if [ -f "${EC2_TARGET_DIR}/execution_agent.py" ]; then
        _SRC_MD5=$(md5sum "${AGENT_FILE}" | cut -d' ' -f1)
        _TGT_MD5=$(md5sum "${EC2_TARGET_DIR}/execution_agent.py" | cut -d' ' -f1)
        if [ "${_SRC_MD5}" != "${_TGT_MD5}" ]; then
            _SRC_LINES=$(wc -l < "${AGENT_FILE}")
            _TGT_LINES=$(wc -l < "${EC2_TARGET_DIR}/execution_agent.py")
            echo -e "${YELLOW}⚠ WARNING: Source (${_SRC_LINES} lines) differs from production (${_TGT_LINES} lines)${NC}"
            if [ "${_SRC_LINES}" -lt "${_TGT_LINES}" ]; then
                echo -e "${RED}❌ ABORT: Source is SMALLER than production — deploy would regress the live file${NC}"
                echo -e "${RED}   Run with FORCE_DEPLOY=1 to override${NC}"
                [ "${FORCE_DEPLOY:-0}" != "1" ] && exit 1
            fi
        fi
    fi
    cp "${AGENT_FILE}" "${EC2_TARGET_DIR}/execution_agent.py"
fi
chmod +x "${EC2_TARGET_DIR}/execution_agent.py"
echo -e "${GREEN}✅ Step 4: Copied to ${EC2_TARGET_DIR}${NC}"
cat > /etc/systemd/system/${SYSTEMD_SERVICE}.service << EOF
[Unit]
Description=Project Neo — Execution Agent (all users)
After=network.target
[Service]
Type=simple
User=root
WorkingDirectory=${EC2_TARGET_DIR}
Environment="PATH=${VENV_PATH}/bin:/usr/local/bin:/usr/bin:/bin"
Environment="AWS_DEFAULT_REGION=${AWS_REGION}"
ExecStart=${VENV_PATH}/bin/python ${EC2_TARGET_DIR}/execution_agent.py
Restart=on-failure
RestartSec=30
StandardOutput=append:${LOG_DIR}/execution_agent.log
StandardError=append:${LOG_DIR}/execution_agent.error.log
[Install]
WantedBy=multi-user.target
EOF
echo -e "${GREEN}✅ Step 5: systemd service created${NC}"
cat > /etc/logrotate.d/neo-execution_agent << EOF
${LOG_DIR}/execution_agent.log ${LOG_DIR}/execution_agent.error.log { daily rotate 14 compress delaycompress missingok notifempty copytruncate }
EOF
echo -e "${GREEN}✅ Step 6: Log rotation configured${NC}"
systemctl daemon-reload; systemctl enable "${SYSTEMD_SERVICE}"; systemctl restart "${SYSTEMD_SERVICE}"; sleep 5
if systemctl is-active --quiet "${SYSTEMD_SERVICE}"; then
    echo -e "${GREEN}✅ Step 7: Service running${NC}"
else
    echo -e "${RED}❌ Step 7: Service failed${NC}"; journalctl -u "${SYSTEMD_SERVICE}" --no-pager -n 20; exit 1
fi
echo ""
echo "=================================================="
echo -e " ${GREEN}DEPLOYMENT COMPLETE${NC}"
echo "=================================================="
echo " Service: ${SYSTEMD_SERVICE}"
echo " Logs:    ${LOG_DIR}/execution_agent.log"
echo " Users:   All active users from risk_parameters"
echo " Commands:"
echo "   systemctl status ${SYSTEMD_SERVICE}"
echo "   journalctl -u ${SYSTEMD_SERVICE} -f"
echo "   tail -f ${LOG_DIR}/execution_agent.log"
echo "=================================================="
