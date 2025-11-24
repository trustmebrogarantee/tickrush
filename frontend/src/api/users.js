export default (axios) => ({
  getUsers() {
    return axios.get('/api/users')
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  getUser(id) {
    return axios.get(`/api/users/${id}`)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  createUser(payload) {
    return axios.post('/api/users', payload)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  updateUser(id, payload) {
    return axios.put(`/api/users/${id}`, payload)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  },
  deleteUser(id) {
    return axios.delete(`/api/users/${id}`)
      .then(response => ({ data: response.data, error: null }))
      .catch(error => ({ data: null, error }));
  }
});
