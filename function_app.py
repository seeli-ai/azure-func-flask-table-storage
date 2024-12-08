import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)



@app.route(route="prime")
def prime(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    primes = [2,3]

    number_as_string = req.params.get('number')

    number = int(number_as_string)

    if number <= len(primes):
        return func.HttpResponse(f"{primes[number-1]} is the {number}th prime number (cached)")
    
    number_to_test = primes[-1] + 2
    
    while len(primes) < number:
        square_root = number_to_test ** 0.5
        for prime in primes:
            if prime > square_root:
                primes.append(number_to_test)
                number_to_test += 2
                break
            if number_to_test % prime == 0:
                number_to_test += 2
                break
            
    return func.HttpResponse(f"{primes[-1]} is the {number}th prime number")
