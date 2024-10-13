<?php
session_start();
require_once 'include/db.php';
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['get_comps'])) {
    // Fetch the input values from the form
    $address = $_POST['address'];
    $propertyType = $_POST['property_type'];
    $bedrooms = $_POST['bedrooms'];
    $bathrooms = $_POST['bathrooms'];
    $squareFootage = $_POST['square_footage'];
    $apiKey = "d4c21d49832f4cf8874cb5f193398482"; // Replace with your API key

    // Construct the API URL
    $url = "https://api.rentcast.io/v1/avm/value?address=" . urlencode($address) . "&propertyType=" . urlencode($propertyType) . "&bedrooms=" . urlencode($bedrooms) . "&bathrooms=" . urlencode($bathrooms) . "&squareFootage=" . urlencode($squareFootage);

    // Call the API
    $headers = [
        "accept: application/json",
        "X-Api-Key: $apiKey"
    ];
    
    $ch = curl_init($url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    $response = curl_exec($ch);
    curl_close($ch);

    // Handle the API response
    if ($response) {
        $apiData = json_decode($response, true);
        $price = $apiData['price'];
        $priceRangeLow = $apiData['priceRangeLow'];
        $priceRangeHigh = $apiData['priceRangeHigh'];
        $comparables = $apiData['comparables'];
    } else {
        $error = "Failed to retrieve data from the API.";
    }
}
?>

<html lang="en">
<head>
    <meta charset="utf-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Get Property Comps</title>
    <link href="../assets/vendor/bootstrap-select/dist/css/bootstrap-select.min.css" rel="stylesheet">
    <link href="../assets/css/style.css" rel="stylesheet">
</head>
<body>

<div id="main-wrapper">
    <?php include 'include/nav.php'; ?>
    <?php include 'include/sidebar.php'; ?>

    <div class="content-body">
        <div class="container-fluid">
            <div class="page-titles">
                <ol class="breadcrumb">
                    <li class="breadcrumb-item"><a href="javascript:void(0)">Admin Panel</a></li>
                    <li class="breadcrumb-item active"><a href="javascript:void(0)">Get Property Comps</a></li>
                </ol>
            </div>

            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-header">
                            <h4 class="card-title">Get Property Comps</h4>
                        </div>
                        <div class="card-body">
                            <form method="POST" action="">
                                <div class="form-group">
                                    <label for="address">Address:</label>
                                    <input type="text" name="address" id="address" class="form-control" required>
                                </div>
                                <div>
                                  <label for="query-valueEstimate_propertyType">Property Type:</label>
                                  <select class="Select Select_sm Param-select3zpmIPk4ibrt rm-ParamSelect" id="property_type" name="property_type">
                                    <option value="">Select a property type</option>
                                    <option value="Single Family">Single Family</option>
                                    <option value="Condo">Condo</option>
                                    <option value="Townhouse">Townhouse</option>
                                    <option value="Manufactured">Manufactured</option>
                                    <option value="Multi-Family">Multi-Family</option>
                                    <option value="Apartment">Apartment</option>
                                    <option value="Land">Land</option>
                                  </select>
                                 </div>
                                <div class="form-group">
                                    <label for="bedrooms">Bedrooms:</label>
                                    <input type="number" name="bedrooms" id="bedrooms" class="form-control" required>
                                </div>
                                <div class="form-group">
                                    <label for="bathrooms">Bathrooms:</label>
                                    <input type="number" name="bathrooms" id="bathrooms" class="form-control" required>
                                </div>
                                <div class="form-group">
                                    <label for="square_footage">Square Footage:</label>
                                    <input type="number" name="square_footage" id="square_footage" class="form-control" required>
                                </div>
                                <button type="submit" name="get_comps" class="btn btn-primary">Get Comps</button>
                            </form>

                            <?php if (isset($apiData)): ?>
                                <hr>
                                <h4>Property Value Estimates</h4>
                                <div class="form-group">
                                    <label for="price">Price Estimate:</label>
                                    <input type="text" class="form-control" value="<?php echo $price; ?>" readonly>
                                </div>
                                <div class="form-group">
                                    <label for="price_range_low">Price Range Low:</label>
                                    <input type="text" class="form-control" value="<?php echo $priceRangeLow; ?>" readonly>
                                </div>
                                <div class="form-group">
                                    <label for="price_range_high">Price Range High:</label>
                                    <input type="text" class="form-control" value="<?php echo $priceRangeHigh; ?>" readonly>
                                </div>

                                <h4>Comparable Properties</h4>
                                <div class="row">
                                    <?php foreach ($comparables as $comparable): ?>
                                        <div class="col-md-4">
                                            <div class="card">
                                                <div class="card-body">
                                                    <h5 class="card-title"><?php echo $comparable['formattedAddress']; ?></h5>
                                                    <p class="card-text">
                                                        Price: $<?php echo $comparable['price']; ?><br>
                                                        Bedrooms: <?php echo $comparable['bedrooms']; ?><br>
                                                        Bathrooms: <?php echo $comparable['bathrooms']; ?><br>
                                                        Square Footage: <?php echo $comparable['squareFootage']; ?>
                                                    </p>
                                                </div>
                                            </div>
                                        </div>
                                    <?php endforeach; ?>
                                </div>
                            <?php elseif (isset($error)): ?>
                                <div class="alert alert-danger"><?php echo $error; ?></div>
                            <?php endif; ?>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <?php include 'include/footer.php'; ?>
</div>

<script src="../assets/vendor/global/global.min.js"></script>
<script src="../assets/vendor/bootstrap-select/dist/js/bootstrap-select.min.js"></script>
<script src="../assets/js/custom.min.js"></script>
<script src="../assets/js/deznav-init.js"></script>

</body>
</html>