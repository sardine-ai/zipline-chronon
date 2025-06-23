"""
Demo and example usage for the TUI module.
"""

from datetime import date
from ai.chronon.cli.tui.models import StepStatus, StepDependency
from ai.chronon.cli.tui.app import InteractiveDAGApp


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
                      "Initial project setup and infrastructure provisioning", []),
            StepStatus(date(2023, 1, 15), date(2023, 1, 28), "finished", 
                      "Core application framework and basic routing", 
                      [("web_app", date(2023, 1, 1), date(2023, 1, 14))]),
            StepStatus(date(2023, 1, 29), date(2023, 2, 11), "running", 
                      "Authentication and authorization implementation", 
                      [("web_app", date(2023, 1, 15), date(2023, 1, 28)), 
                       ("auth_service", date(2023, 1, 6), date(2023, 1, 20))]),
            StepStatus(date(2023, 2, 12), date(2023, 2, 25), "finished", 
                      "API endpoints and business logic development", 
                      [("web_app", date(2023, 1, 29), date(2023, 2, 11)), 
                       ("backend", date(2023, 2, 4), date(2023, 2, 10))]),
            StepStatus(date(2023, 2, 26), date(2023, 3, 11), "running", 
                      "Load testing and performance optimization", 
                      [("web_app", date(2023, 2, 12), date(2023, 2, 25))]),
            StepStatus(date(2023, 3, 12), date(2023, 3, 25), "finished", 
                      "Production deployment and monitoring setup", 
                      [("web_app", date(2023, 2, 26), date(2023, 3, 11))]),
            StepStatus(date(2023, 3, 26), date(2023, 3, 31), "finished", 
                      "Final documentation and handover", 
                      [("web_app", date(2023, 3, 12), date(2023, 3, 25))])
        ],
        "backend": [
            StepStatus(date(2023, 1, 3), date(2023, 1, 17), "running", 
                      "Database schema design and initial migration", 
                      [("database", date(2023, 1, 2), date(2023, 1, 16))]),
            StepStatus(date(2023, 1, 18), date(2023, 2, 3), "failed", 
                      "Connection pool issues and timeout problems", 
                      [("backend", date(2023, 1, 3), date(2023, 1, 17))]),
            StepStatus(date(2023, 2, 4), date(2023, 2, 10), "running", 
                      "Database connection fixes and retry logic", 
                      [("backend", date(2023, 1, 18), date(2023, 2, 3))]),
            StepStatus(date(2023, 2, 11), date(2023, 2, 24), "finished", 
                      "API gateway configuration and rate limiting", 
                      [("backend", date(2023, 2, 4), date(2023, 2, 10))]),
            StepStatus(date(2023, 2, 25), date(2023, 3, 10), "running", 
                      "Microservices orchestration and service mesh", 
                      [("backend", date(2023, 2, 11), date(2023, 2, 24))]),
            StepStatus(date(2023, 3, 11), date(2023, 3, 24), "finished", 
                      "Backend monitoring and logging infrastructure", 
                      [("backend", date(2023, 2, 25), date(2023, 3, 10))]),
            StepStatus(date(2023, 3, 25), date(2023, 3, 31), "finished", 
                      "Performance tuning and optimization", 
                      [("backend", date(2023, 3, 11), date(2023, 3, 24))])
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


def create_dag_based_example():
    """Create example data where child nodes depend on corresponding parent step ranges."""
    dependencies = {
        "frontend": "web_app",
        "backend": "web_app", 
        "database": "backend",
        "cache": "backend",
        "auth_service": "backend",
        "user_service": "auth_service"
    }
    root = "web_app"
    date_range = (date(2023, 1, 1), date(2023, 3, 31))
    
    # Create step statuses with DAG-based dependencies
    node_statuses = {
        "web_app": [
            StepStatus(date(2023, 1, 1), date(2023, 1, 14), "finished", 
                      "Project setup", ()),
            StepStatus(date(2023, 1, 15), date(2023, 1, 28), "finished", 
                      "Core framework", 
                      (StepDependency("web_app", date(2023, 1, 1), date(2023, 1, 14)),)),
            StepStatus(date(2023, 1, 29), date(2023, 2, 11), "running", 
                      "Integration layer", 
                      (StepDependency("web_app", date(2023, 1, 15), date(2023, 1, 28)),)),
        ],
        "frontend": [
            StepStatus(date(2023, 1, 5), date(2023, 1, 19), "running", 
                      "UI components", 
                      (StepDependency("web_app", date(2023, 1, 1), date(2023, 1, 14)),)),
            StepStatus(date(2023, 1, 20), date(2023, 2, 5), "running", 
                      "Frontend integration", 
                      (StepDependency("web_app", date(2023, 1, 15), date(2023, 1, 28)),
                       StepDependency("frontend", date(2023, 1, 5), date(2023, 1, 19)))),
        ],
        "backend": [
            StepStatus(date(2023, 1, 3), date(2023, 1, 17), "running", 
                      "API design", 
                      (StepDependency("web_app", date(2023, 1, 1), date(2023, 1, 14)),)),
            StepStatus(date(2023, 1, 18), date(2023, 2, 3), "failed", 
                      "Service implementation", 
                      (StepDependency("web_app", date(2023, 1, 15), date(2023, 1, 28)),
                       StepDependency("backend", date(2023, 1, 3), date(2023, 1, 17)))),
        ],
        "database": [
            StepStatus(date(2023, 1, 10), date(2023, 1, 24), "running", 
                      "Schema setup", 
                      (StepDependency("backend", date(2023, 1, 3), date(2023, 1, 17)),)),
            StepStatus(date(2023, 1, 25), date(2023, 2, 10), "running", 
                      "Data migration", 
                      (StepDependency("backend", date(2023, 1, 18), date(2023, 2, 3)),
                       StepDependency("database", date(2023, 1, 10), date(2023, 1, 24)))),
        ],
        "auth_service": [
            StepStatus(date(2023, 1, 12), date(2023, 1, 26), "running", 
                      "Auth setup", 
                      (StepDependency("backend", date(2023, 1, 3), date(2023, 1, 17)),)),
            StepStatus(date(2023, 1, 27), date(2023, 2, 12), "running", 
                      "Security implementation", 
                      (StepDependency("backend", date(2023, 1, 18), date(2023, 2, 3)),
                       StepDependency("auth_service", date(2023, 1, 12), date(2023, 1, 26)))),
        ],
        "user_service": [
            StepStatus(date(2023, 1, 20), date(2023, 2, 5), "running", 
                      "User management", 
                      (StepDependency("auth_service", date(2023, 1, 12), date(2023, 1, 26)),)),
            StepStatus(date(2023, 2, 6), date(2023, 2, 20), "running", 
                      "User features", 
                      (StepDependency("auth_service", date(2023, 1, 27), date(2023, 2, 12)),
                       StepDependency("user_service", date(2023, 1, 20), date(2023, 2, 5)))),
        ],
        "cache": [
            StepStatus(date(2023, 1, 15), date(2023, 1, 30), "running", 
                      "Cache layer", 
                      (StepDependency("backend", date(2023, 1, 3), date(2023, 1, 17)),)),
        ]
    }
    
    return dependencies, root, date_range, node_statuses


def run_example():
    """Run the example interactive visualizer."""
    dependencies, root, date_range, node_statuses = create_dag_based_example()
    app = InteractiveDAGApp(dependencies, root, date_range, node_statuses)
    app.run()


def create_test_data():
    """Create test data specifically for testing blocking steps algorithm."""
    dependencies = {
        "service_b": "service_a", 
        "service_c": "service_a"
    }
    root = "service_a"
    date_range = (date(2023, 1, 1), date(2023, 2, 28))
    
    node_statuses = {
        "service_a": [
            StepStatus(date(2023, 1, 1), date(2023, 1, 10), "finished", "Setup", ()),
            StepStatus(date(2023, 1, 11), date(2023, 1, 20), "running", "Development", ()),
            StepStatus(date(2023, 1, 21), date(2023, 1, 30), "running", "Testing", 
                      (StepDependency("service_a", date(2023, 1, 11), date(2023, 1, 20)),)),
        ],
        "service_b": [
            StepStatus(date(2023, 1, 5), date(2023, 1, 15), "running", "Initial work", 
                      (StepDependency("service_a", date(2023, 1, 1), date(2023, 1, 10)),)),
            StepStatus(date(2023, 1, 16), date(2023, 1, 25), "running", "Integration", 
                      (StepDependency("service_a", date(2023, 1, 11), date(2023, 1, 20)),
                       StepDependency("service_b", date(2023, 1, 5), date(2023, 1, 15)))),
        ],
        "service_c": [
            StepStatus(date(2023, 1, 12), date(2023, 1, 22), "running", "Parallel work", 
                      (StepDependency("service_a", date(2023, 1, 11), date(2023, 1, 20)),)),
            StepStatus(date(2023, 1, 23), date(2023, 2, 5), "running", "Final phase", 
                      (StepDependency("service_c", date(2023, 1, 12), date(2023, 1, 22)),
                       StepDependency("service_b", date(2023, 1, 16), date(2023, 1, 25)))),
        ]
    }
    
    return dependencies, root, date_range, node_statuses


def test_blocking_steps():
    """Test cases for the blocking steps algorithm."""
    from ai.chronon.cli.tui.widgets import ProgressGrid
    
    dependencies, root, date_range, node_statuses = create_test_data()
    grid = ProgressGrid(dependencies, root, date_range, node_statuses)
    
    print("=== Blocking Steps Algorithm Test Cases ===\n")
    
    # Test Case 1: No dependencies (first step)
    print("Test Case 1: No dependencies")
    target_step = node_statuses["service_a"][0]  # Setup step
    blocking = grid._get_blocking_steps("service_a", target_step)
    print(f"Target: service_a Setup ({target_step.start_date} to {target_step.end_date})")
    print(f"Blocking steps: {len(blocking)}")
    for node, step in blocking:
        print(f"  - {node}: {step.details} ({step.start_date} to {step.end_date}) [{step.status}]")
    print()
    
    # Test Case 2: Direct dependency (exact match)
    print("Test Case 2: Direct dependency - exact match")
    target_step = node_statuses["service_b"][0]  # Initial work
    blocking = grid._get_blocking_steps("service_b", target_step)
    print(f"Target: service_b Initial work ({target_step.start_date} to {target_step.end_date})")
    print(f"Dependencies: {[(dep.node_name, dep.start_date, dep.end_date) for dep in target_step.step_dependencies]}")
    print(f"Blocking steps: {len(blocking)}")
    for node, step in blocking:
        print(f"  - {node}: {step.details} ({step.start_date} to {step.end_date}) [{step.status}]")
    print()
    
    # Test Case 3: Overlapping dependency
    print("Test Case 3: Overlapping dependency")
    target_step = node_statuses["service_c"][0]  # Parallel work
    blocking = grid._get_blocking_steps("service_c", target_step)
    print(f"Target: service_c Parallel work ({target_step.start_date} to {target_step.end_date})")
    print(f"Dependencies: {[(dep.node_name, dep.start_date, dep.end_date) for dep in target_step.step_dependencies]}")
    print(f"Blocking steps: {len(blocking)}")
    for node, step in blocking:
        print(f"  - {node}: {step.details} ({step.start_date} to {step.end_date}) [{step.status}]")
    print()
    
    # Test Case 4: Transitive dependencies (recursive)
    print("Test Case 4: Transitive dependencies")
    target_step = node_statuses["service_c"][1]  # Final phase
    blocking = grid._get_blocking_steps("service_c", target_step)
    print(f"Target: service_c Final phase ({target_step.start_date} to {target_step.end_date})")
    print(f"Dependencies: {[(dep.node_name, dep.start_date, dep.end_date) for dep in target_step.step_dependencies]}")
    print(f"Blocking steps: {len(blocking)}")
    for node, step in blocking:
        print(f"  - {node}: {step.details} ({step.start_date} to {step.end_date}) [{step.status}]")
    print()
    
    # Test Case 5: Chain of dependencies
    print("Test Case 5: Testing step with chain of dependencies")
    target_step = node_statuses["service_a"][2]  # Testing
    blocking = grid._get_blocking_steps("service_a", target_step)
    print(f"Target: service_a Testing ({target_step.start_date} to {target_step.end_date})")
    print(f"Dependencies: {[(dep.node_name, dep.start_date, dep.end_date) for dep in target_step.step_dependencies]}")
    print(f"Blocking steps: {len(blocking)}")
    for node, step in blocking:
        print(f"  - {node}: {step.details} ({step.start_date} to {step.end_date}) [{step.status}]")
    print()


if __name__ == "__main__":
    # Run tests
    test_blocking_steps()