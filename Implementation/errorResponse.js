function generateErrorResponse(missingFields) {
    return {
        "status": "error",
        "statusCode": 400,
        "message": "Bad Request",
        "error": {
            "type": "Invalid Headers",
            "description": `${missingFields.join(' & ')} is missing - Pass ${missingFields.join(' & ')} in the query parameters.`
          }
    };
}

function generateSuccessResponse(message) {
    return {
        "status": "Success",
        "statusCode": 200,
        "response": message,
    };
}

module.exports = {
    generateErrorResponse,
    generateSuccessResponse
};
