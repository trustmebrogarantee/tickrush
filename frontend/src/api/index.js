import axios from 'axios';
import users from './users';
import symbols from './symbols';

const instance = axios.create({
  baseURL: '/',
  timeout: 10000,
  withCredentials: true,
});

export default {
  users: users(instance),
  symbols: symbols(instance),
};
