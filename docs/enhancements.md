I want to identify issues with the library in contrast to the broker API documentation and fix them.
The doucmentation is available at the following locations.
Guides: https://docs.tradier.com/docs
API Reference: https://docs.tradier.com/reference

I want the library to be able to call the API on behalf of a caller and return the response to the caller. If there is an error, the library should report the error back to the called.

The API also puts rate limits restrictions on API calls for endpoint groups per minute. These rate limits are also documented in the documentation.

I want the library to refuse starting calls to the API if the rate limit for a particular endpoint is exhausted and is not refreshed for the minute yet. Or if the call is initiated and the API responds with a rate limit error, report the same to the caller.