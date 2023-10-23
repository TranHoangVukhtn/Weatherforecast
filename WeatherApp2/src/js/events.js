/* eslint-disable no-alert */
/* eslint-disable no-console */
import dom from './dom';
const csvtojson = require('csvtojson');

const events = function events() {
  function showFlow(data) {
    dom().clearForms();
    dom().fillCard(data);
    dom().imageSwitch(data, 'image');
    dom().show('search');

    const farCel = document.getElementById('farCel');
    farCel.onclick = function changeTemp() { dom().converter(data); };
  }

  function forecastFlow(data) {
    dom().clearForms();
    dom().createCard(data);
    dom().show('forecast');
  }

  async function getSearch(city) {
    try {
      const url = `https://api.openweathermap.org/data/2.5/weather?q=${city}&APPID=903507f17d707fecd352d38301efba77&units=metric`;
      const response = await fetch(url, { mode: 'cors' });
      const cityData = await response.json();
      showFlow(cityData);
    } catch (error) {
      console.error('Error:', error);
      alert('Could not find the location');
    }
  }

  function getLocation(searchBar) {
    const city = (document.getElementById(searchBar).value).toLowerCase();
    getSearch(city);
  }

  async function getForecast() {
    try {
      const value = (document.getElementById('search').value).toLowerCase();
      const url = `https://api.openweathermap.org/data/2.5/forecast?q=${value}&units=metric&appid=903507f17d707fecd352d38301efba77`;
      const response = await fetch(url, { mode: 'cors' });
      const cityData = await response.json();
      forecastFlow(cityData);
    } catch (error) {
      console.error('Error:', error);
      alert('Could not find the location');
    }
  }
async function getForecast2() {
    try {
        const value = (document.getElementById('search').value).toLowerCase();
        const filePath = 'D:/DE/forecasted.csv'; // Thay đổi đường dẫn đến tệp CSV trên ổ C:
        const columnsToRead = ["maxtemp_c", "maxtemp_f", "mintemp_c", "avgtemp_c", "condition"];
        const data = [];

        const fs = require('fs');
        const csv = require('csv-parser');

        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => {
                const rowData = {};
                columnsToRead.forEach((column) => {
                    rowData[column] = row[column];
                });
                data.push(rowData);
            })
            .on('end', () => {
                console.log('Dữ liệu từ tệp CSV:', data);
                // Ở đây, bạn có thể xử lý dữ liệu hoặc hiển thị nó.
                const cityData =  csvtojson().fromFile(filePath);
                forecastFlow(cityData);
            });

    } catch (error) {
        console.error('Lỗi:', error);
        alert('Không thể tìm thấy địa điểm');
    }
}



  return {
    getSearch, showFlow, getForecast, getLocation,
  };
};

export { events as default };