export default (axios) => ({
  getSymbols() {
    return axios.get('/api/symbols')
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  getSymbol(id) {
    return axios.get(`/api/symbols/${id}`)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  createSymbol(payload) {
    return axios.post('/api/symbols', payload)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  updateSymbol(id, payload) {
    return axios.put(`/api/symbols/${id}`, payload)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  deleteSymbol(id) {
    return axios.delete(`/api/symbols/${id}`)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  // Market CRUD for a symbol
  getMarkets(symbolId) {
    return axios.get(`/api/symbols/${symbolId}/markets`)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  createMarket(symbolId, payload) {
    return axios.post(`/api/symbols/${symbolId}/markets`, payload)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  updateMarket(symbolId, marketId, payload) {
    return axios.put(`/api/symbols/${symbolId}/markets/${marketId}`, payload)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  deleteMarket(symbolId, marketId) {
    return axios.delete(`/api/symbols/${symbolId}/markets/${marketId}`)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  }
});
