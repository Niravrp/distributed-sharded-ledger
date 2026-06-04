$GatewayUrl = "https://api-router-eastus.happytree-0d70414b.eastus.azurecontainerapps.io/transaction"
$TotalRequests = 5000
$Concurrency = 50

$StartTime = Get-Date
Write-Host "🚀 INITIATING SUSTAINED CHAOS: $TotalRequests requests to force KEDA polling..." -ForegroundColor Cyan
Write-Host "🕒 Start Time: $($StartTime.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor DarkGray

# Define the payload generator block
$ScriptBlock = {
    param($Url, $i)
    $accountId = "load_user_$($i % 10)" 
    $body = @{ accountId = $accountId; amount = 10.00; currency = "USD" } | ConvertTo-Json
    
    try {
        $response = Invoke-RestMethod -Uri $Url -Method Post -Body $body -ContentType "application/json" -TimeoutSec 15
        return "[SUCCESS] tx: $($response.transactionId)"
    } catch {
        return "[FAILED] Request $i dropped."
    }
}

# Fire the requests in massive parallel batches
$Jobs = @()
for ($i = 1; $i -le $TotalRequests; $i++) {
    $Jobs += Start-ThreadJob -ScriptBlock $ScriptBlock -ArgumentList $GatewayUrl, $i
    
    # Throttle slightly to maintain concurrency limit
    if (($Jobs | Where-Object State -eq 'Running').Count -ge $Concurrency) {
        $Jobs | Wait-Job -Any | Out-Null
    }
}

Write-Host "⏳ Traffic sent! Waiting for final threads to resolve..." -ForegroundColor Yellow

# Robust Wait Loop
$Jobs | Wait-Job | Out-Null
$Results = $Jobs | Receive-Job
$Jobs | Remove-Job

# Tally the metrics
$SuccessCount = ($Results | Where-Object { $_ -match "SUCCESS" }).Count
$FailCount = ($Results | Where-Object { $_ -match "FAILED" }).Count

$EndTime = Get-Date
$Duration = $EndTime - $StartTime

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "✅ SUSTAINED TEST COMPLETE!" -ForegroundColor Green
Write-Host "🕒 End Time: $($EndTime.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor DarkGray
Write-Host "⏱️ Total Duration: $($Duration.Minutes) minutes and $($Duration.Seconds) seconds" -ForegroundColor Magenta
Write-Host "Successful: $SuccessCount"
Write-Host "Dropped: $FailCount"
Write-Host "=====================================" -ForegroundColor Cyan