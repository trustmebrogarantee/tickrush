<script setup>
import api from '@/api';
console.log('API:', api);

const createSymbol = async () => {
    const response = await api.users.createUser({ username: 'BTCUSDT', email: 'btc@example.com' });
    console.log('Symbol created:', response);
};

</script>

<template>
<h1>Template</h1>
<button @click="createSymbol">create symbol</button>
</template>