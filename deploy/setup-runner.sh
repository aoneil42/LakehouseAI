#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# setup-runner.sh — One-time EC2 instance setup for GitLab CI/CD
#
# Run this on the target EC2 instance to install Docker and the GitLab Runner.
# After running, register the runner with your GitLab instance.
#
# Usage:
#   ssh ec2-user@<ip> 'bash -s' < deploy/setup-runner.sh
#
# Then register the runner:
#   ssh ec2-user@<ip>
#   sudo gitlab-runner register \
#     --url https://gitlab.example.com \
#     --registration-token <TOKEN> \
#     --executor shell \
#     --description "terminus-ec2" \
#     --tag-list "terminus,deploy,ec2"
###############################################################################

echo "══════════════════════════════════════════"
echo "  Terminus GIS — EC2 Runner Setup"
echo "══════════════════════════════════════════"

# ── Detect OS ─────────────────────────────────────────────────────────
if [ -f /etc/os-release ]; then
  . /etc/os-release
  OS_ID="${ID}"
else
  echo "Cannot detect OS. Exiting."
  exit 1
fi

# ── Install Docker ────────────────────────────────────────────────────
echo ""
echo "① Installing Docker..."
if command -v docker &> /dev/null; then
  echo "   Docker already installed: $(docker --version)"
else
  if [ "$OS_ID" = "amzn" ]; then
    # Amazon Linux 2 / 2023
    sudo yum install -y docker
    sudo systemctl enable docker
    sudo systemctl start docker
  elif [ "$OS_ID" = "ubuntu" ]; then
    sudo apt-get update
    sudo apt-get install -y docker.io docker-compose-plugin
    sudo systemctl enable docker
    sudo systemctl start docker
  else
    echo "   Unsupported OS: ${OS_ID}. Install Docker manually."
    exit 1
  fi
  echo "   ✓ Docker installed."
fi

# ── Install Docker Compose plugin (if not bundled) ────────────────────
echo ""
echo "② Checking Docker Compose..."
if docker compose version &> /dev/null; then
  echo "   Docker Compose available: $(docker compose version --short)"
else
  echo "   Installing Docker Compose plugin..."
  COMPOSE_VERSION="v2.29.1"
  ARCH=$(uname -m)
  sudo mkdir -p /usr/local/lib/docker/cli-plugins
  sudo curl -SL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-${ARCH}" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
  echo "   ✓ Docker Compose installed."
fi

# ── Install GitLab Runner ─────────────────────────────────────────────
echo ""
echo "③ Installing GitLab Runner..."
if command -v gitlab-runner &> /dev/null; then
  echo "   GitLab Runner already installed: $(gitlab-runner --version | head -1)"
else
  curl -L "https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.rpm.sh" | sudo bash
  sudo yum install -y gitlab-runner 2>/dev/null || sudo apt-get install -y gitlab-runner
  echo "   ✓ GitLab Runner installed."
fi

# ── Add gitlab-runner to docker group ─────────────────────────────────
echo ""
echo "④ Configuring permissions..."
sudo usermod -aG docker gitlab-runner
echo "   ✓ gitlab-runner added to docker group."

# ── Install git-lfs (for large files) ─────────────────────────────────
echo ""
echo "⑤ Installing git-lfs..."
if command -v git-lfs &> /dev/null; then
  echo "   git-lfs already installed."
else
  sudo yum install -y git-lfs 2>/dev/null || sudo apt-get install -y git-lfs
  echo "   ✓ git-lfs installed."
fi

# ── Done ──────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════"
echo "  ✅  Setup complete!"
echo "══════════════════════════════════════════"
echo ""
echo "  Next steps:"
echo ""
echo "  1. Register the runner:"
echo "     sudo gitlab-runner register \\"
echo "       --url https://gitlab.example.com \\"
echo "       --registration-token <TOKEN> \\"
echo "       --executor shell \\"
echo "       --description 'terminus-ec2' \\"
echo "       --tag-list 'terminus,deploy,ec2'"
echo ""
echo "  2. Verify:"
echo "     sudo gitlab-runner list"
echo "     sudo gitlab-runner verify"
echo ""
