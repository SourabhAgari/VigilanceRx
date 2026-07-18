resource "google_billing_budget" "trial_guard" {
  billing_account = var.billing_account_id
  display_name = "rx-vigilance-trial-guard"

  budget_filter {
    projects = ["projects/${data.google_project.current.number}"]
  }

  amount {
    specified_amount {
      currency_code = "INR"
      units = "25000"
    }
  }

  threshold_rules {
    threshold_percent = 0.2
  }
  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.8
  }
  threshold_rules {
    threshold_percent = 1.0
  }
}

data "google_project" "current" {
  project_id = var.project_id
}