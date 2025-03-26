async function make_n_async_requests_to_url({how_many, url}) {
    const prom_requests = []
    for (let i = 1; i <= how_many; i++) {
        let prom_request = fetch(url)
        prom_requests.push(prom_request)
    }
    return Promise.all(prom_requests)
}


await make_n_async_requests_to_url({
    how_many: 10,
    url: "<your url  here>"
})
