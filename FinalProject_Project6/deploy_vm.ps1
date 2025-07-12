# Variables
$resourceGroup = "SparkProjectResourceGroup"
$vmName = "sparkLinuxVM"
$location = "centralus"
$image = "Ubuntu2204"
$username = "azureuser"
$vmSize = "Standard_D2as_v5"  # Pick a smaller size to avoid availability issues

# Check if logged in by seeing if any subscriptions are enabled
$subs = az account list --query "[?state=='Enabled']" --output tsv 2>$null

if (-not $subs) {
    Write-Host "Not logged in to Azure. Logging in now..."
    az login
} else {
    Write-Host "Already logged in to Azure."
}

# Create resource group if it doesn't exist
Write-Host "Creating resource group '$resourceGroup' in location '$location' (if it does not exist)..."
az group create --name $resourceGroup --location $location | Out-Null

# Check if VM exists
Write-Host "Checking if VM '$vmName' exists in resource group '$resourceGroup'..."
$vmExists = az vm show --resource-group $resourceGroup --name $vmName --query "name" --output tsv 2>$null

if ($vmExists) {
    Write-Host "VM '$vmName' exists. Deleting..."
    az vm delete --yes --resource-group $resourceGroup --name $vmName | Out-Null
    Write-Host "VM deleted."
}

# Create the VM
Write-Host "Creating VM '$vmName'..."
$createVmResult = az vm create `
    --resource-group $resourceGroup `
    --name $vmName `
    --image $image `
    --admin-username $username `
    --authentication-type ssh `
    --generate-ssh-keys `
    --location $location `
    --size $vmSize `
    --output json 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Error "VM creation failed with error: $createVmResult"
    exit 1
}

# Get public IP
$publicIp = az vm show -d -g $resourceGroup -n $vmName --query publicIps -o tsv

Write-Host ""
Write-Host "VM '$vmName' has been created successfully!"
Write-Host ""
Write-Host "To connect to your VM via SSH, run the following command:"
Write-Host ""
Write-Host "    ssh $username@$publicIp"
Write-Host ""
Write-Host "SSH keys were saved to your local machine (usually ~/.ssh/id_rsa or equivalent)."
