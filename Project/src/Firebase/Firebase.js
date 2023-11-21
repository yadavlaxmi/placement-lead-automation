import { initializeApp } from "firebase/app";

const firebaseConfig = {
    apiKey: "AIzaSyAKsBR65c8HDmPWmoXuPWMjEVFXli_8SOQ",
    authDomain: "fir-75f86.firebaseapp.com",
    projectId: "fir-75f86",
    storageBucket: "fir-75f86.appspot.com",
    messagingSenderId: "840974213469",
    appId: "1:840974213469:web:a34960496e0958107e88b4",
    databaseURL:"https://fir-75f86-default-rtdb.firebaseio.com/"
  };
export const app = initializeApp(firebaseConfig);  