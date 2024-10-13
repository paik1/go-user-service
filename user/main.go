package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

var db *sql.DB

// Config struct for holding configuration
type Config struct {
	Database struct {
		ConnectionString string `json:"connection_string"`
	} `json:"database"`
	Azure struct {
		BlobConnectionString       string `json:"blob_connection_string"`
		ServiceBusConnectionString string `json:"service_bus_connection_string"`
	} `json:"azure"`
}

// User struct for the API
type User struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Link      string    `json:"link"`
	CreatedAt time.Time `json:"createdAt"`
}

func toPtr[T any](v T) *T {
	return &v
}

func initDB(config Config) {
	var err error
	db, err = sql.Open("sqlserver", config.Database.ConnectionString) // Use "sqlserver" for Azure SQL
	if err != nil {
		log.Fatalf("Error connecting to the database: %v\n", err)
	}

	// Check if the database is reachable
	if err = db.Ping(); err != nil {
		log.Fatalf("Cannot ping the database: %v\n", err)
	}
	log.Println("Successfully connected to the Azure SQL Database!")
}

// Azure Blob Upload Handler
func uploadToBlobStorage(file io.Reader, filename string, config Config) (string, error) {
	blobServiceClient, err := azblob.NewClientFromConnectionString(config.Azure.BlobConnectionString, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create blob client: %v", err)
	}

	blobURL := fmt.Sprintf("%s/%s", "profile-pictures", filename)
	_, err = blobServiceClient.UploadStream(context.TODO(), "profile-pictures", filename, file, &azblob.UploadStreamOptions{
		Metadata: map[string]*string{
			"ContentType": toPtr("image/jpeg"), // Set content type using pointer to string
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload to blob: %v", err)
	}

	return blobURL, nil
}

// Send User Data to Azure Service Bus
func sendToServiceBus(user User, config Config) error {
	client, err := azservicebus.NewClientFromConnectionString(config.Azure.ServiceBusConnectionString, nil)
	if err != nil {
		return fmt.Errorf("failed to create service bus client: %v", err)
	}
	defer client.Close(context.TODO())

	sender, err := client.NewSender("user-queue", nil)
	if err != nil {
		return fmt.Errorf("failed to create sender: %v", err)
	}
	defer sender.Close(context.TODO())

	// Marshal the user data into JSON format
	userData, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to marshal user data: %v", err)
	}

	// Send the message to the Service Bus queue
	message := &azservicebus.Message{
		Body: userData,
	}
	err = sender.SendMessage(context.TODO(), message, nil)
	if err != nil {
		return fmt.Errorf("failed to send message to service bus: %v", err)
	}

	log.Printf("User data sent to Service Bus: %s", string(userData))
	return nil
}

// API to Create a New User (POST /users)
func createUser(w http.ResponseWriter, r *http.Request, config Config) {
	// Parse form data
	name := r.FormValue("name")
	email := r.FormValue("email")
	file, header, err := r.FormFile("photo")
	if err != nil {
		http.Error(w, "Invalid file upload", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Upload profile picture to Azure Blob Storage
	profilePicURL, err := uploadToBlobStorage(file, header.Filename, config)
	if err != nil {
		log.Printf("Error uploading file to blob storage: %v", err)
		http.Error(w, "Error uploading file", http.StatusInternalServerError)
		return
	}

	// Prepare user data
	user := User{
		Name:  name,
		Email: email,
		Link:  profilePicURL,
	}

	// Send user data to Service Bus
	err = sendToServiceBus(user, config)
	if err != nil {
		log.Printf("Error sending user data to Service Bus: %v", err)
		http.Error(w, "Error sending user data", http.StatusInternalServerError)
		return
	}

	// Respond with success message
	json.NewEncoder(w).Encode(map[string]string{
		"message":         "User created successfully",
		"profile_pic_url": profilePicURL,
	})
}

// API to Get All Users (GET /users)
func getUsers(w http.ResponseWriter) {
	rows, err := db.Query(`SELECT * FROM users`)
	if err != nil {
		log.Printf("Error fetching users from database: %v", err)
		http.Error(w, "Error fetching users", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		if err := rows.Scan(&user.ID, &user.Name, &user.Email, &user.Link, &user.CreatedAt); err != nil {
			log.Printf("Error scanning row: %v", err)
			http.Error(w, "Error scanning user data", http.StatusInternalServerError)
			return
		}
		users = append(users, user)
	}

	json.NewEncoder(w).Encode(users)
}

func main() {
	// Load configuration from JSON file
	configFile, err := os.Open("config.json")
	if err != nil {
		log.Fatalf("Error opening config file: %v", err)
	}
	defer configFile.Close()

	var config Config
	if err := json.NewDecoder(configFile).Decode(&config); err != nil {
		log.Fatalf("Error decoding config file: %v", err)
	}

	// Initialize database
	initDB(config)
	defer db.Close()

	// Define routes
	r := mux.NewRouter()
	r.HandleFunc("/users", func(w http.ResponseWriter, r *http.Request) {
		getUsers(w)
	}).Methods("GET")
	r.HandleFunc("/users", func(w http.ResponseWriter, r *http.Request) {
		createUser(w, r, config)
	}).Methods("POST")

	// Create a new CORS handler
	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:3000"}, // Allow your frontend URL
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type"},
		AllowCredentials: true, // Allow credentials if needed
	})

	// Start server with CORS middleware
	log.Println("Starting server on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", corsHandler.Handler(r)))
}
