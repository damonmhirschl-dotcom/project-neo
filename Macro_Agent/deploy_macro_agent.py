#!/usr/bin/env python3
"""
Project Neo - Macro Agent Deployment Script
===========================================

Deploys the macro agent to the production environment with proper validation
and safety checks.

Deployment Steps:
1. Validate environment and prerequisites  
2. Run complete test suite
3. Deploy agent files to EC2
4. Set up systemd service (optional)
5. Perform post-deployment validation

Usage:
    python deploy_macro_agent.py --validate-only
    python deploy_macro_agent.py --deploy --target-host ec2-user@10.50.2.177
    python deploy_macro_agent.py --deploy --local-test
"""

import os
import sys
import argparse
import subprocess
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - DEPLOY - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MacroAgentDeployer:
    """Handles deployment of the macro agent."""
    
    def __init__(self):
        self.project_files = [
            'macro_agent.py',
            'test_macro_agent.py'
        ]
        self.deployment_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    def validate_environment(self) -> bool:
        """Validate local environment and files."""
        logger.info("=== Validating Environment ===")
        
        try:
            # Check Python version
            python_version = sys.version_info
            if python_version < (3, 8):
                logger.error(f"❌ Python 3.8+ required, found {python_version.major}.{python_version.minor}")
                return False
            
            logger.info(f"✅ Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
            
            # Check required files exist
            for file in self.project_files:
                if not Path(file).exists():
                    logger.error(f"❌ Required file missing: {file}")
                    return False
                logger.info(f"✅ Found: {file}")
            
            # Check required Python packages
            required_packages = [
                'boto3', 'psycopg2', 'anthropic', 'requests'
            ]
            
            missing_packages = []
            for package in required_packages:
                try:
                    __import__(package)
                    logger.info(f"✅ Package: {package}")
                except ImportError:
                    missing_packages.append(package)
            
            if missing_packages:
                logger.error(f"❌ Missing packages: {missing_packages}")
                logger.info("Install with: pip install boto3 psycopg2-binary anthropic requests")
                return False
            
            logger.info("✅ Environment validation PASSED")
            return True
            
        except Exception as e:
            logger.error(f"❌ Environment validation FAILED: {e}")
            return False
    
    def run_test_suite(self) -> bool:
        """Run the complete test suite."""
        logger.info("=== Running Test Suite ===")
        
        try:
            # Run the test suite
            result = subprocess.run([
                sys.executable, 'test_macro_agent.py', '--all'
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info("✅ Test suite PASSED")
                logger.info(result.stdout)
                return True
            else:
                logger.error("❌ Test suite FAILED")
                logger.error(result.stderr)
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Test suite TIMEOUT after 5 minutes")
            return False
        except Exception as e:
            logger.error(f"❌ Test suite execution FAILED: {e}")
            return False
    
    def deploy_files(self, target_host: str = None) -> bool:
        """Deploy files to target environment."""
        logger.info(f"=== Deploying Files ===")
        
        if target_host:
            return self._deploy_remote(target_host)
        else:
            return self._deploy_local()
    
    def _deploy_local(self) -> bool:
        """Deploy for local testing."""
        logger.info("Deploying for local testing...")
        
        try:
            # Create deployment directory
            deploy_dir = Path(f"./neo_macro_agent_{self.deployment_timestamp}")
            deploy_dir.mkdir(exist_ok=True)
            
            # Copy files
            import shutil
            for file in self.project_files:
                src = Path(file)
                dst = deploy_dir / file
                shutil.copy2(src, dst)
                logger.info(f"✅ Copied {file} to {dst}")
            
            # Create startup script
            startup_script = deploy_dir / "start_macro_agent.sh"
            startup_script.write_text(f"""#!/bin/bash
cd {deploy_dir.absolute()}
python3 macro_agent.py --continuous
""")
            startup_script.chmod(0o755)
            
            logger.info(f"✅ Local deployment completed in {deploy_dir}")
            logger.info(f"To run: cd {deploy_dir} && python3 macro_agent.py --single")
            return True
            
        except Exception as e:
            logger.error(f"❌ Local deployment FAILED: {e}")
            return False
    
    def _deploy_remote(self, target_host: str) -> bool:
        """Deploy to remote EC2 instance."""
        logger.info(f"Deploying to remote host: {target_host}")
        
        try:
            # Create deployment package
            package_name = f"neo_macro_agent_{self.deployment_timestamp}.tar.gz"
            
            # Create tar package
            subprocess.run([
                'tar', 'czf', package_name
            ] + self.project_files, check=True)
            
            logger.info(f"✅ Created deployment package: {package_name}")
            
            # Upload package
            subprocess.run([
                'scp', package_name, f"{target_host}:~/"
            ], check=True)
            
            logger.info("✅ Uploaded deployment package")
            
            # Extract and setup on remote
            remote_commands = f"""
                tar xzf {package_name}
                mkdir -p ~/neo_agents
                mv macro_agent.py test_macro_agent.py ~/neo_agents/
                cd ~/neo_agents
                python3 test_macro_agent.py --config-only
            """
            
            subprocess.run([
                'ssh', target_host, remote_commands
            ], check=True)
            
            logger.info("✅ Remote deployment completed")
            logger.info(f"Files deployed to {target_host}:~/neo_agents/")
            
            # Clean up local package
            os.remove(package_name)
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Remote deployment FAILED: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Remote deployment FAILED: {e}")
            return False
    
    def create_systemd_service(self, target_host: str = None) -> bool:
        """Create systemd service for the macro agent (optional)."""
        logger.info("=== Creating Systemd Service ===")
        
        service_content = """[Unit]
Description=Project Neo - Macro Agent
After=network.target
Wants=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/neo_agents
ExecStart=/usr/bin/python3 macro_agent.py --continuous
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""
        
        if target_host:
            try:
                # Create service file locally first
                with open('neo-macro-agent.service', 'w') as f:
                    f.write(service_content)
                
                # Upload and install service
                subprocess.run(['scp', 'neo-macro-agent.service', f"{target_host}:~/"], check=True)
                
                remote_commands = """
                    sudo mv ~/neo-macro-agent.service /etc/systemd/system/
                    sudo systemctl daemon-reload
                    sudo systemctl enable neo-macro-agent
                    echo "Service installed. To start: sudo systemctl start neo-macro-agent"
                    echo "To check status: sudo systemctl status neo-macro-agent"
                """
                
                subprocess.run(['ssh', target_host, remote_commands], check=True)
                
                # Clean up local file
                os.remove('neo-macro-agent.service')
                
                logger.info("✅ Systemd service created")
                return True
                
            except Exception as e:
                logger.error(f"❌ Systemd service creation FAILED: {e}")
                return False
        else:
            # Just create the service file locally
            with open('neo-macro-agent.service', 'w') as f:
                f.write(service_content)
            logger.info("✅ Created neo-macro-agent.service (install manually)")
            return True
    
    def post_deployment_validation(self, target_host: str = None) -> bool:
        """Run post-deployment validation."""
        logger.info("=== Post-Deployment Validation ===")
        
        try:
            if target_host:
                # Run remote test
                result = subprocess.run([
                    'ssh', target_host, 'cd ~/neo_agents && python3 test_macro_agent.py --config-only'
                ], capture_output=True, text=True, timeout=60)
                
                if result.returncode == 0:
                    logger.info("✅ Remote validation PASSED")
                    return True
                else:
                    logger.error("❌ Remote validation FAILED")
                    logger.error(result.stderr)
                    return False
            else:
                # Local validation already done in test suite
                logger.info("✅ Local validation completed in test suite")
                return True
                
        except Exception as e:
            logger.error(f"❌ Post-deployment validation FAILED: {e}")
            return False
    
    def full_deployment(self, target_host: str = None, create_service: bool = False) -> bool:
        """Run complete deployment process."""
        logger.info("🚀 Starting macro agent deployment")
        logger.info(f"Deployment timestamp: {self.deployment_timestamp}")
        
        steps = [
            ("Environment Validation", self.validate_environment),
            ("Test Suite", self.run_test_suite),
            ("File Deployment", lambda: self.deploy_files(target_host)),
            ("Post-Deployment Validation", lambda: self.post_deployment_validation(target_host))
        ]
        
        if create_service:
            steps.append(("Systemd Service", lambda: self.create_systemd_service(target_host)))
        
        for step_name, step_func in steps:
            logger.info(f"Starting: {step_name}")
            
            try:
                if not step_func():
                    logger.error(f"💥 Deployment FAILED at step: {step_name}")
                    return False
                    
                logger.info(f"✅ Completed: {step_name}")
                
            except Exception as e:
                logger.error(f"💥 Step {step_name} threw exception: {e}")
                return False
        
        logger.info("🎉 Macro agent deployment completed successfully!")
        
        if target_host:
            logger.info(f"""
Next steps:
1. SSH to {target_host}
2. cd ~/neo_agents
3. Run single test: python3 macro_agent.py --single
4. If successful, start continuous: python3 macro_agent.py --continuous
            """)
        
        return True

def main():
    parser = argparse.ArgumentParser(description='Deploy Project Neo Macro Agent')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only validate environment and run tests')
    parser.add_argument('--deploy', action='store_true',
                       help='Deploy the agent')
    parser.add_argument('--target-host', type=str,
                       help='Target host for deployment (e.g., ec2-user@10.50.2.177)')
    parser.add_argument('--local-test', action='store_true',
                       help='Deploy locally for testing')
    parser.add_argument('--create-service', action='store_true',
                       help='Create systemd service (requires --deploy)')
    
    args = parser.parse_args()
    
    deployer = MacroAgentDeployer()
    
    try:
        if args.validate_only:
            success = (deployer.validate_environment() and 
                      deployer.run_test_suite())
            
        elif args.deploy:
            if args.local_test and args.target_host:
                logger.error("Cannot specify both --local-test and --target-host")
                sys.exit(1)
            
            target = args.target_host if not args.local_test else None
            success = deployer.full_deployment(target, args.create_service)
            
        else:
            # Default to validation only
            success = (deployer.validate_environment() and 
                      deployer.run_test_suite())
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Deployment interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
