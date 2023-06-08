<?php
header('Access-Control-Allow-Origin: *');

$databaseFile = '../../database/college_tennis.db';

$dataTable = array();
$error = '';
$response_code = 200;

// Get the GET data

//$id = $_GET['id'];

try {
    $conn = new PDO( "sqlite:$databaseFile");
} catch (Exception $e) {
    $error = $e->getMessage();
    $response_code = 500;
}

$conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

                          
$query = "SELECT utr_id, ita_team_id, name, division, gender, conference, 
            city, state, latLong, team_url, twitter_link, facebook_link, instagram_link \n".
            "FROM college; \n";
            //"WHERE id ='".$id."'";

try {
    $result = $conn->query( $query);
    while($row = $result->fetch(PDO::FETCH_ASSOC)){
            $dataTable[]=$row ;
        }
} catch (Exception $e) {
    $error =  $e->getMessage();
    $response_code = 500;
}

$response = array(
    'data'=>$dataTable,
    'query'=>$query,
    'error'=>$error);
http_response_code($response_code);
echo json_encode($response);
exit();
?>