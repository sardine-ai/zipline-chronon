"""
Demo and example usage for the TUI module.
"""

from datetime import date
from .models import StepStatus
from .app import InteractiveDAGApp


def create_example_data():
    """Create example data for demonstration."""
    dependencies = {
        "frontend": "web_app",
        "backend": "web_app",
        "database": "backend",
        "cache": "backend",
        "auth_service": "backend",
        "user_service": "auth_service"
    }
    root = "web_app"
    date_range = (date(2023, 1, 1), date(2023, 3, 31))  # 90 days (Jan 1 - Mar 31)
    
    # Example status data with details - 90 day timeline
    node_statuses = {
        "web_app": [
            StepStatus(date(2023, 1, 1), date(2023, 1, 14), "running", 
                      "Initial project setup and infrastructure provisioning"),
            StepStatus(date(2023, 1, 15), date(2023, 1, 28), "finished", 
                      "Core application framework and basic routing"),
            StepStatus(date(2023, 1, 29), date(2023, 2, 11), "running", 
                      "Authentication and authorization implementation"),
            StepStatus(date(2023, 2, 12), date(2023, 2, 25), "finished", 
                      "API endpoints and business logic development"),
            StepStatus(date(2023, 2, 26), date(2023, 3, 11), "running", 
                      "Load testing and performance optimization"),
            StepStatus(date(2023, 3, 12), date(2023, 3, 25), "finished", 
                      "Production deployment and monitoring setup"),
            StepStatus(date(2023, 3, 26), date(2023, 3, 31), "finished", 
                      "Final documentation and handover")
        ],
        "backend": [
            StepStatus(date(2023, 1, 3), date(2023, 1, 17), "running", 
                      "Database schema design and initial migration"),
            StepStatus(date(2023, 1, 18), date(2023, 2, 3), "failed", 
                      "Connection pool issues and timeout problems"),
            StepStatus(date(2023, 2, 4), date(2023, 2, 10), "running", 
                      "Database connection fixes and retry logic"),
            StepStatus(date(2023, 2, 11), date(2023, 2, 24), "finished", 
                      "API gateway configuration and rate limiting"),
            StepStatus(date(2023, 2, 25), date(2023, 3, 10), "running", 
                      "Microservices orchestration and service mesh"),
            StepStatus(date(2023, 3, 11), date(2023, 3, 24), "finished", 
                      "Backend monitoring and logging infrastructure"),
            StepStatus(date(2023, 3, 25), date(2023, 3, 31), "finished", 
                      "Performance tuning and optimization")
        ],
        "frontend": [
            StepStatus(date(2023, 1, 5), date(2023, 1, 19), "finished", 
                      "React component library and design system"),
            StepStatus(date(2023, 1, 20), date(2023, 2, 5), "running", 
                      "User interface implementation and styling"),
            StepStatus(date(2023, 2, 6), date(2023, 2, 19), "finished", 
                      "State management and data flow implementation"),
            StepStatus(date(2023, 2, 20), date(2023, 3, 5), "running", 
                      "Integration testing with backend APIs"),
            StepStatus(date(2023, 3, 6), date(2023, 3, 19), "finished", 
                      "User acceptance testing and bug fixes"),
            StepStatus(date(2023, 3, 20), date(2023, 3, 31), "finished", 
                      "Performance optimization and accessibility")
        ],
        "database": [
            StepStatus(date(2023, 1, 2), date(2023, 1, 16), "running", 
                      "Initial database cluster setup and configuration"),
            StepStatus(date(2023, 1, 17), date(2023, 1, 30), "finished", 
                      "Data migration from legacy systems"),
            StepStatus(date(2023, 1, 31), date(2023, 2, 13), "running", 
                      "Performance tuning and query optimization"),
            StepStatus(date(2023, 2, 14), date(2023, 2, 27), "finished", 
                      "Backup and disaster recovery setup"),
            StepStatus(date(2023, 2, 28), date(2023, 3, 13), "running", 
                      "Database monitoring and alerting"),
            StepStatus(date(2023, 3, 14), date(2023, 3, 31), "finished", 
                      "Security hardening and compliance checks")
        ],
        "auth_service": [
            StepStatus(date(2023, 1, 6), date(2023, 1, 20), "running", 
                      "OAuth2 and SAML integration development"),
            StepStatus(date(2023, 1, 21), date(2023, 2, 3), "finished", 
                      "User management and role-based access control"),
            StepStatus(date(2023, 2, 4), date(2023, 2, 17), "running", 
                      "Multi-factor authentication implementation"),
            StepStatus(date(2023, 2, 18), date(2023, 3, 3), "finished", 
                      "Security audit and penetration testing"),
            StepStatus(date(2023, 3, 4), date(2023, 3, 17), "running", 
                      "Session management and token validation"),
            StepStatus(date(2023, 3, 18), date(2023, 3, 31), "finished", 
                      "Production deployment and monitoring")
        ],
        "user_service": [
            StepStatus(date(2023, 1, 8), date(2023, 1, 22), "running", 
                      "User profile management and preferences"),
            StepStatus(date(2023, 1, 23), date(2023, 2, 5), "finished", 
                      "User activity tracking and analytics"),
            StepStatus(date(2023, 2, 6), date(2023, 2, 19), "running", 
                      "User notification system and templates"),
            StepStatus(date(2023, 2, 20), date(2023, 3, 5), "finished", 
                      "User data export and privacy compliance"),
            StepStatus(date(2023, 3, 6), date(2023, 3, 31), "finished", 
                      "User service optimization and caching")
        ],
        "cache": [
            StepStatus(date(2023, 1, 10), date(2023, 1, 24), "finished", 
                      "Redis cluster setup and configuration"),
            StepStatus(date(2023, 1, 25), date(2023, 2, 7), "running", 
                      "Cache warming strategies and data preloading"),
            StepStatus(date(2023, 2, 8), date(2023, 2, 21), "finished", 
                      "Cache invalidation patterns and TTL optimization"),
            StepStatus(date(2023, 2, 22), date(2023, 3, 7), "running", 
                      "Distributed caching and consistency protocols"),
            StepStatus(date(2023, 3, 8), date(2023, 3, 21), "finished", 
                      "Cache monitoring and performance metrics"),
            StepStatus(date(2023, 3, 22), date(2023, 3, 31), "finished", 
                      "Cache cluster scaling and failover testing")
        ]
    }
    
    return dependencies, root, date_range, node_statuses


def run_example():
    """Run the example interactive visualizer."""
    dependencies, root, date_range, node_statuses = create_example_data()
    app = InteractiveDAGApp(dependencies, root, date_range, node_statuses)
    app.run()