# XPChex - Complete Offline Deployment Package

A complete dockerized solution for deploying XPChex (Python FastAPI backend + Next.js frontend + PostgreSQL) on offline RHEL servers.

## 🏗️ Architecture

- **Backend**: Python FastAPI with PostgreSQL
- **Frontend**: Next.js React application
- **Database**: PostgreSQL (local, no external dependencies)
- **Containerization**: Docker & Docker Compose
- **Deployment**: Offline RHEL server compatible

## 📦 What's Included

### Backend (Python FastAPI)
- FastAPI application with review analysis capabilities
- PostgreSQL database integration
- Google Play Store scraper
- LLM integration for review analysis
- RESTful API endpoints

### Frontend (Next.js)
- Modern React application
- Responsive UI with Tailwind CSS
- Real-time data visualization
- API integration with backend

### Database (PostgreSQL)
- Local PostgreSQL instance
- Pre-configured with required tables
- Database initialization scripts
- Optimized for offline deployment

## 🚀 Quick Start

### Prerequisites
- RHEL server (offline capable)
- Root/sudo access
- At least 4GB RAM
- 10GB free disk space

### Installation

1. **Transfer the deployment package** to your RHEL server
2. **Extract the package**:
   ```bash
   tar -xzf xpchex-deployment-*.tar.gz
   cd xpchex-deployment-*
   ```

3. **Load Docker images**:
   ```bash
   ./load-images.sh
   ```

4. **Run automated installation**:
   ```bash
   sudo ./install.sh
   ```

### Access Points

After installation:
- **Frontend**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **Database**: localhost:5432

## 🔧 Configuration

### Environment Variables

The application uses the following environment variables:

```bash
# Database Configuration
PGHOST=postgres
PGPORT=5432
PGDATABASE=xpchex
PGUSER=xpchex_user
PGPASSWORD=xpchex_password
DB_SSL_MODE=disable

# Application Configuration
NEXT_PUBLIC_API_URL=http://localhost:8000
```

### Database Credentials

- **Database**: xpchex
- **User**: xpchex_user
- **Password**: xpchex_password
- **Host**: localhost
- **Port**: 5432

## 📋 Management Commands

### Service Management
```bash
# Check service status
systemctl status xpchex

# View logs
journalctl -u xpchex -f

# Stop service
systemctl stop xpchex

# Start service
systemctl start xpchex

# Restart service
systemctl restart xpchex
```

### Docker Management
```bash
# View running containers
docker ps

# View container logs
docker compose logs

# Stop all services
docker compose down

# Start all services
docker compose up -d
```

## 🛠️ Development

### Local Development Setup

1. **Clone and setup**:
   ```bash
   git clone <repository>
   cd xpchex-backend-new
   ```

2. **Start development environment**:
   ```bash
   docker compose up -d
   ```

3. **Access services**:
   - Frontend: http://localhost:3000
   - Backend: http://localhost:8000
   - Database: localhost:5432

### Backend Development

```bash
cd backend
pip install -r requirements-clean.txt
uvicorn main:app --reload
```

### Frontend Development

```bash
cd frontend
npm install
npm run dev
```

## 📊 Features

### Backend Features
- Google Play Store review scraping
- Review sentiment analysis
- AI-powered review summarization
- RESTful API endpoints
- Database persistence
- Real-time data processing

### Frontend Features
- Modern responsive UI
- Real-time data visualization
- Interactive charts and graphs
- Review analysis dashboard
- Mobile-friendly design

## 🔒 Security

### Production Considerations
- Change default database passwords
- Configure firewall rules
- Use SSL certificates for HTTPS
- Implement proper authentication
- Regular security updates

### Network Configuration
- Frontend: Port 3000
- Backend: Port 8000
- Database: Port 5432
- All services communicate via Docker network

## 🐛 Troubleshooting

### Common Issues

1. **Services not starting**:
   ```bash
   systemctl status xpchex
   journalctl -u xpchex -f
   ```

2. **Database connection issues**:
   ```bash
   docker compose logs postgres
   ```

3. **Frontend not loading**:
   ```bash
   docker compose logs frontend
   ```

4. **Backend API errors**:
   ```bash
   docker compose logs backend
   ```

### Log Locations
- System logs: `journalctl -u xpchex -f`
- Docker logs: `docker compose logs`
- Application logs: Inside containers

## 📈 Monitoring

### Health Checks
- Backend: http://localhost:8000/
- Frontend: http://localhost:3000/
- Database: Connection test via backend

### Performance Monitoring
```bash
# Container resource usage
docker stats

# System resource usage
htop
```

## 🔄 Updates

### Updating the Application
1. Stop the service: `systemctl stop xpchex`
2. Update the code/images
3. Restart the service: `systemctl start xpchex`

### Database Migrations
Database migrations are handled automatically during container startup.

## 📞 Support

For issues or questions:
1. Check the logs first
2. Review this documentation
3. Check Docker and system status
4. Contact the development team

## 📄 License

This project is proprietary software. All rights reserved.

---

**XPChex Deployment Package** - Complete offline deployment solution for RHEL servers.
