const express = require('express');
const {
    Sequelize,
    DataTypes
} = require('sequelize');
const fs = require('fs');
const readline = require('readline');
const os = require('os');
const {
    execSync
} = require('child_process');

const app = express();
const CHUNK_SIZE = 100000000; // Размер части файла (например, 100 МБ)

// Инициализация Sequelize с указанием пути к файлу SQLite
const sequelize = new Sequelize({
    dialect: 'sqlite',
    storage: 'travel-data.sqlite' // Путь к вашему файлу SQLite
});

const Flights = sequelize.define('flights', {
    flight_id: {
        type: DataTypes.INTEGER,
        allowNull: false
    },
    flight_no: {
        type: DataTypes.STRING,
        allowNull: false
    },
    scheduled_departure: {
        type: DataTypes.DATE,
        allowNull: false
    },
    scheduled_arrival: {
        type: DataTypes.DATE,

        allowNull: false
    },
    departure_airport: {
        type: DataTypes.STRING,
        allowNull: false
    },
    arrival_airport: {
        type: DataTypes.STRING,
        allowNull: false
    },
    status: {
        type: DataTypes.STRING,
        allowNull: false
    },
    aircraft_code: {
        type: DataTypes.DATE,
        allowNull: false
    },
    actual_departure: {
        type: DataTypes.DATE,
        allowNull: false
    },
}, {
    timestamps: false,
});

// Пример чтения данных из таблицы aircrafts_data
async function readData() {
    try {
        // Синхронизация модели с базой данных
        await Flights.sync();

        // Чтение данных из таблицы aircrafts_data
        const airData = await Flights.findAll({
            attributes: [
                'flight_id',
                'flight_no',
                'scheduled_departure',
                'scheduled_arrival',
                'departure_airport',
                'arrival_airport',
                'status',
                'aircraft_code',
                'actual_departure'
            ],
            raw: true,
        });
        console.log('Список пользователей:', airData);
        return airData;
    } catch (error) {
        console.error('Ошибка чтения данных:', airData);
        return [];
    }
}

async function sortLargeFile(filePath) {
    const tempFiles = [];
    let count = 0;

    const input = fs.createReadStream(filePath, {
        encoding: 'utf8'
    });
    const rl = readline.createInterface({
        input
    });

    let lines = [];
    for await (const line of rl) {
        lines.push(line);
        count++;

        if (count % CHUNK_SIZE === 0) {
            lines = lines.sort(); // Сортировка строк в памяти
            const tempFilePath = `temp_${tempFiles.length}.txt`;
            tempFiles.push(tempFilePath);

            fs.writeFileSync(tempFilePath, lines.join(os.EOL)); // Запись отсортированных строк во временный файл
            lines = [];
        }
    }

    // Обработка оставшихся строк
    if (lines.length > 0) {
        lines = lines.sort();
        const tempFilePath = `temp_${tempFiles.length}.txt`;
        tempFiles.push(tempFilePath);
        fs.writeFileSync(tempFilePath, lines.join(os.EOL));
    }

    // Слияние отсортированных временных файлов
    const sortedFilePath = 'sorted_file.txt';
    const mergeCommand = `sort -m ${tempFiles.join(' ')} -o ${sortedFilePath}`;
    execSync(mergeCommand); // Выполнение команды для слияния файлов

    // Очистка временных файлов
    tempFiles.forEach((file) => fs.unlinkSync(file));

    console.log('Файл успешно отсортирован:', sortedFilePath);
}

app.get("/docs/", async (req, res) => {
    try {
        const flights_data = await readData(); // Предположим, что у вас есть функция readData(), возвращающая JSON
        const jsonData = JSON.stringify({
            flights_data
        });

        const dataForTXT = flights_data.map(index => `${index.flight_no} ${index.scheduled_arrival} ${index.departure_airport} ${index.arrival_airport}`).join('\n');

        // Запись данных в файл, в дальнейшем его прочтем.
        fs.writeFile('flights_data.txt', dataForTXT, err => {
            if (err) {
                res.status(500).json({
                    error: 'Ошибка при записи данных в файл'
                });
            } else {
                sortLargeFile('flights_data.txt');
                res.json({
                    success: 'Данные успешно записаны в файл flights_data.txt и отсортированы.'
                });
            }
        });
    } catch (error) {
        res.status(500).json({
            error: 'Ошибка при чтении данных'
        });
    }
});

const PORT = process.env.PORT || 5000;

app.listen(PORT, () => {
    console.log(`Server started on port ${PORT}`);
});
