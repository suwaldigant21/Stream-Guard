# Phase 9 — production observability: SNS alerts + CloudWatch metric alarms.
#
# Budget note: 3 basic CloudWatch alarms at $0.10 each/mo + 4 custom metrics
# (first 10 free) + SNS email (free tier) ≈ $0.30/mo — comfortably inside the
# plan's $3-5 no-top-up cap.
#
# Email subscription is deliberately NOT managed here (protocol = "email"):
# (1) the HashiCorp AWS provider cannot track the out-of-band confirmation
# lifecycle, so AWS-side deletes drift from terraform.tfstate (plan reports
# "No changes" while AWS shows the subscription gone), and (2) email security
# scanners that pre-fetch the Unsubscribe link silently purge the subscription
# right after confirmation. Subscribe out-of-band instead:
#   scripts/setup_sns_subscription.ps1 -Email you@example.com

# 1. SNS topic for high-severity fraud & system alerts.
resource "aws_sns_topic" "streamguard_alerts" {
  name = "streamguard-high-severity-alerts"
}

# 2. Alarm: persistent consumer scoring failures (scorer down / HTTP errors).
resource "aws_cloudwatch_metric_alarm" "consumer_error_alarm" {
  alarm_name          = "streamguard-consumer-scoring-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ScorerErrorCount"
  namespace           = "StreamGuard/Inference"
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Triggers when the streaming consumer encounters persistent scoring HTTP failures (> 5 in 5 min)."
  alarm_actions       = [aws_sns_topic.streamguard_alerts.arn]
}

# 3. Alarm: abnormally high fraud density (spike detection).
resource "aws_cloudwatch_metric_alarm" "fraud_spike_alarm" {
  alarm_name          = "streamguard-high-fraud-rate-spike"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "FraudAlertCount"
  namespace           = "StreamGuard/Inference"
  period              = 300
  statistic           = "Sum"
  threshold           = 50
  alarm_description   = "Triggers when detected fraud spikes abnormally high over a 5-minute window (> 50 alerts in 5 min)."
  alarm_actions       = [aws_sns_topic.streamguard_alerts.arn]
}

# 4. Alarm (P1-4): consumer-heartbeat liveness — the "dead-man's switch".
# The consumer pushes one ConsumerHeartbeat Count every HEARTBEAT_PERIOD_S of
# wall-clock time. A dead / hung consumer stops emitting datapoints entirely;
# treat_missing_data = "breaching" makes missing minutes count as zeros, so 3
# consecutive minute-periods of silence trip the alarm. This closes the Phase 9
# "silent zero" blind spot (the two workload alarms only fire when the consumer
# is ALIVE and producing data).
resource "aws_cloudwatch_metric_alarm" "consumer_heartbeat_alarm" {
  alarm_name          = "streamguard-consumer-heartbeat"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ConsumerHeartbeat"
  namespace           = "StreamGuard/Inference"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_description   = "Triggers when no consumer heartbeat lands for 3 consecutive minutes (dead or hung consumer)."
  alarm_actions       = [aws_sns_topic.streamguard_alerts.arn]
}
