$resourceGroup = "SparkProjectResourceGroup"
$vmName = "sparkLinuxVM"

# Delete the VM (if it exists)
if (az vm show --resource-group $resourceGroup --name $vmName -o none 2>$null) {
    Write-Host "Deleting VM '$vmName'..."
    az vm delete --resource-group $resourceGroup --name $vmName --yes --no-wait
} else {
    Write-Host "VM '$vmName' does not exist or is already deleted."
}

# Delete the resource group (this deletes everything inside it, including disks, NICs, IPs)
Write-Host "Deleting resource group '$resourceGroup' and all its resources..."
az group delete --name $resourceGroup --yes --no-wait

Write-Host "Deletion initiated. It may take some time for Azure to complete the cleanup."
