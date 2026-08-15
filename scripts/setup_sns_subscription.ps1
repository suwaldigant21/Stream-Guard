# StreamGuard Phase 9 - provision the SNS email subscription OUT of band.
#
# The email subscription is deliberately NOT in Terraform: with protocol =
# "email" the provider cannot track the out-of-band confirmation lifecycle
# (AWS-side deletes drift from state) and email scanners that pre-fetch the
# Unsubscribe link silently purge the subscription right after confirmation.
# Use this script after `terraform apply` instead.
#
# Usage:
#   .\scripts\setup_sns_subscription.ps1 -Email you@example.com
# Then click "Confirm subscription" in the AWS notification email promptly.

param(
    [Parameter(Mandatory = $true)]
    [string]$Email
)

$region = if ($env:AWS_REGION) { $env:AWS_REGION } else { "us-east-1" }

$topicArn = aws sns list-topics --query "Topics[].TopicArn" --output text --region $region 2>$null |
    Where-Object { $_ -like "*streamguard-high-severity-alerts*" } | Select-Object -First 1

if (-not $topicArn) {
    Write-Error "SNS topic 'streamguard-high-severity-alerts' not found in region $region. Run terraform apply first."
    exit 1
}

Write-Host "Subscribing $Email to $topicArn ..."
aws sns subscribe --topic-arn $topicArn --protocol email --notification-endpoint $Email --region $region

Write-Host ""
Write-Host "Confirmation email sent. Open the 'AWS Notification - Subscription Confirmation'"
Write-Host "email and click 'Confirm subscription' promptly (an email scanner that pre-fetches"
Write-Host "the Unsubscribe link will silently delete the subscription)."
Write-Host ""
Write-Host "Verify afterwards with:"
Write-Host "  aws sns get-topic-attributes --topic-arn $topicArn --query `"Attributes.[SubscriptionsPending,SubscriptionsConfirmed,SubscriptionsDeleted]`" --output text"
