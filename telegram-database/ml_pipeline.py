import re
import json
import logging
from typing import Dict, Any, List, Tuple
import openai
import config
from database.database import DatabaseManager

class MLPipeline:
    def __init__(self):
        self.db = DatabaseManager()
        
        # Initialize OpenAI client only if API key is available
        self.openai_client = None
        if config.OPENAI_API_KEY:
            try:
                self.openai_client = openai.OpenAI(api_key=config.OPENAI_API_KEY)
                logging.info("✅ OpenAI client initialized successfully")
            except Exception as e:
                logging.warning(f"⚠️ Failed to initialize OpenAI client: {e}")
        else:
            logging.info("⚠️ OpenAI API key not provided, using fallback classification")
        
        # Keywords for different scoring categories
        self.salary_keywords = [
            'salary', 'package', 'lpa', 'ctc', 'stipend', 'pay', 'compensation',
            'rs', 'rupees', 'dollars', '$', '₹', 'k', 'thousand', 'lakh'
        ]
        
        self.contact_keywords = [
            'contact', 'email', 'phone', 'whatsapp', 'call', 'reach out',
            '@', 'gmail.com', 'yahoo.com', 'outlook.com', '+91', '+1'
        ]
        
        self.website_keywords = [
            'website', 'apply', 'careers', 'jobs', 'linkedin', 'indeed',
            'naukri', 'monster', 'glassdoor', 'http', 'www', '.com'
        ]
        
        self.skill_keywords = [
            'python', 'javascript', 'java', 'react', 'angular', 'vue', 'node.js',
            'django', 'flask', 'html', 'css', 'sql', 'mongodb', 'aws', 'docker'
        ]
        
        self.experience_keywords = [
            'experience', 'years', 'fresher', 'entry level', 'junior', 'senior',
            '0-1', '1-2', '2-3', '3-5', '5+', 'entry', 'mid', 'lead'
        ]
        
        self.location_keywords = [
            'remote', 'work from home', 'wfh', 'hybrid', 'onsite', 'bangalore',
            'mumbai', 'delhi', 'pune', 'chennai', 'hyderabad', 'india', 'usa'
        ]
    
    def classify_message(self, message_text: str) -> Dict[str, Any]:
        """Classify if a message is a job post and extract relevant information"""
        try:
            # Use OpenAI for classification if available
            if self.openai_client:
                response = self.openai_client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a job classification expert. Analyze the message and determine if it's a job posting. If yes, extract: job title, company, skills required, experience level, location, salary, contact info, and website. Return as JSON."
                        },
                        {
                            "role": "user",
                            "content": f"Analyze this message: {message_text}"
                        }
                    ],
                    max_tokens=500
                )
                
                result = response.choices[0].message.content
                try:
                    return json.loads(result)
                except:
                    return self._fallback_classification(message_text)
            else:
                return self._fallback_classification(message_text)
                
        except Exception as e:
            logging.error(f"OpenAI classification failed: {e}")
            return self._fallback_classification(message_text)
    
    def _fallback_classification(self, message_text: str) -> Dict[str, Any]:
        """Fallback classification using rule-based approach"""
        text_lower = message_text.lower()
        
        # Check if it's likely a job post
        job_indicators = [
            'hiring', 'job', 'position', 'vacancy', 'opening', 'recruitment',
            'apply', 'candidate', 'developer', 'engineer', 'programmer'
        ]
        
        is_job_post = any(indicator in text_lower for indicator in job_indicators)
        
        if not is_job_post:
            return {
                'is_job_post': False,
                'confidence': 0.1,
                'extracted_data': {}
            }
        
        # Extract basic information
        extracted_data = {
            'job_title': self._extract_job_title(text_lower),
            'company': self._extract_company(text_lower),
            'skills': self._extract_skills(text_lower),
            'experience': self._extract_experience(text_lower),
            'location': self._extract_location(text_lower),
            'salary': self._extract_salary(text_lower),
            'contact': self._extract_contact(text_lower),
            'website': self._extract_website(text_lower)
        }
        
        return {
            'is_job_post': True,
            'confidence': 0.7,
            'extracted_data': extracted_data
        }
    
    def score_message(self, message_text: str, extracted_data: Dict[str, Any]) -> Dict[str, float]:
        """Score a message based on 10 parameters"""
        text_lower = message_text.lower()
        
        scores = {
            'salary_score': self._calculate_salary_score(text_lower, extracted_data),
            'contact_score': self._calculate_contact_score(text_lower, extracted_data),
            'website_score': self._calculate_website_score(text_lower, extracted_data),
            'name_score': self._calculate_name_score(text_lower, extracted_data),
            'skill_score': self._calculate_skill_score(text_lower, extracted_data),
            'experience_score': self._calculate_experience_score(text_lower, extracted_data),
            'location_score': self._calculate_location_score(text_lower, extracted_data),
            'remote_score': self._calculate_remote_score(text_lower, extracted_data),
            'fresher_friendly_score': self._calculate_fresher_friendly_score(text_lower, extracted_data),
            'overall_score': 0.0
        }
        
        # Calculate overall score (average of all scores)
        scores['overall_score'] = sum(scores.values()) / len(scores)
        
        return scores
    
    def _calculate_salary_score(self, text: str, data: Dict[str, Any]) -> float:
        """Calculate salary information score (0-10)"""
        score = 0.0
        
        # Check for salary keywords
        if any(keyword in text for keyword in self.salary_keywords):
            score += 3.0
        
        # Check extracted salary data
        if data.get('salary'):
            score += 4.0
        
        # Check for specific salary ranges
        if re.search(r'\d+[kkl]', text):
            score += 3.0
        
        return min(score, 10.0)
    
    def _calculate_contact_score(self, text: str, data: Dict[str, Any]) -> float:
        """Calculate contact information score (0-10)"""
        score = 0.0
        
        # Check for contact keywords
        if any(keyword in text for keyword in self.contact_keywords):
            score += 3.0
        
        # Check extracted contact data
        if data.get('contact'):
            score += 4.0
        
        # Check for email patterns
        if re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text):
            score += 3.0
        
        return min(score, 10.0)
    
    def _calculate_website_score(self, text: str, data: Dict[str, Any]) -> float:
        """Calculate website/application score (0-10)"""
        score = 0.0
        
        # Check for website keywords
        if any(keyword in text for keyword in self.website_keywords):
            score += 3.0
        
        # Check extracted website data
        if data.get('website'):
            score += 4.0
        
        # Check for URL patterns
        if re.search(r'https?://[^\s]+', text):
            score += 3.0
        
        return min(score, 10.0)
    
    def _calculate_name_score(self, text: str, data: Dict[str, Any]) -> float:
        """Calculate company name score (0-10)"""
        score = 0.0
        
        if data.get('company'):
            score += 5.0
        
        # Check for company indicators
        if re.search(r'(inc|corp|llc|ltd|pvt|company)', text):
            score += 3.0
        
        # Check for job title
        if data.get('job_title'):
            score += 2.0
        
        return min(score, 10.0)
    
    def _calculate_skill_score(self, text: str, data: Dict[str, Any]) -> float:
        """Calculate skills requirement score (0-10)"""
        score = 0.0
        
        # Check for programming language keywords
        if any(keyword in text for keyword in self.skill_keywords):
            score += 4.0
        
        # Check extracted skills
        if data.get('skills'):
            score += 3.0
        
        # Check for multiple skills
        skill_count = len([skill for skill in self.skill_keywords if skill in text])
        score += min(skill_count * 1.5, 3.0)
        
        return min(score, 10.0)
    
    def _calculate_experience_score(self, text: str, data: Dict[str, Any]) -> float:
        """Calculate experience requirement score (0-10)"""
        score = 0.0
        
        # Check for experience keywords
        if any(keyword in text for keyword in self.experience_keywords):
            score += 4.0
        
        # Check extracted experience
        if data.get('experience'):
            score += 3.0
        
        # Check for specific year ranges
        if re.search(r'\d+[-+]\d+', text):
            score += 3.0
        
        return min(score, 10.0)
    
    def _calculate_location_score(self, text: str, data: Dict[str, Any]) -> float:
        """Calculate location information score (0-10)"""
        score = 0.0
        
        # Check for location keywords
        if any(keyword in text for keyword in self.location_keywords):
            score += 4.0
        
        # Check extracted location
        if data.get('location'):
            score += 3.0
        
        # Check for city names
        cities = ['bangalore', 'mumbai', 'delhi', 'pune', 'chennai', 'hyderabad']
        if any(city in text for city in cities):
            score += 3.0
        
        return min(score, 10.0)
    
    def _calculate_remote_score(self, text: str, data: Dict[str, Any]) -> float:
        """Calculate remote work score (0-10)"""
        score = 0.0
        
        remote_indicators = ['remote', 'work from home', 'wfh', 'hybrid']
        if any(indicator in text for indicator in remote_indicators):
            score += 7.0
        
        # Check extracted location
        if data.get('location') and 'remote' in str(data['location']).lower():
            score += 3.0
        
        return min(score, 10.0)
    
    def _calculate_fresher_friendly_score(self, text: str, data: Dict[str, Any]) -> float:
        """Calculate fresher-friendly score (0-10)"""
        score = 0.0
        
        fresher_indicators = ['fresher', 'entry level', 'junior', '0-1 years', 'internship']
        if any(indicator in text for indicator in fresher_indicators):
            score += 6.0
        
        # Check for HTML/CSS/JS (fresher-friendly skills)
        fresher_skills = ['html', 'css', 'javascript', 'js']
        if any(skill in text for skill in fresher_skills):
            score += 4.0
        
        # Check extracted experience
        if data.get('experience') and 'fresher' in str(data['experience']).lower():
            score += 3.0
        
        return min(score, 10.0)
    
    def _extract_job_title(self, text: str) -> str:
        """Extract job title from text"""
        job_titles = ['developer', 'engineer', 'programmer', 'analyst', 'consultant']
        for title in job_titles:
            if title in text:
                return title.title()
        return ""
    
    def _extract_company(self, text: str) -> str:
        """Extract company name from text"""
        # Simple extraction - can be improved
        return ""
    
    def _extract_skills(self, text: str) -> List[str]:
        """Extract required skills from text"""
        found_skills = []
        for skill in self.skill_keywords:
            if skill in text:
                found_skills.append(skill)
        return found_skills
    
    def _extract_experience(self, text: str) -> str:
        """Extract experience requirement from text"""
        experience_patterns = [
            r'\d+[-+]\d+\s*years?',
            r'fresher',
            r'entry\s*level',
            r'junior',
            r'senior'
        ]
        
        for pattern in experience_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group()
        return ""
    
    def _extract_location(self, text: str) -> str:
        """Extract location from text"""
        cities = ['bangalore', 'mumbai', 'delhi', 'pune', 'chennai', 'hyderabad']
        for city in cities:
            if city in text:
                return city.title()
        return ""
    
    def _extract_salary(self, text: str) -> str:
        """Extract salary information from text"""
        salary_patterns = [
            r'\d+[kkl]\s*per\s*annum',
            r'\d+[kkl]\s*lpa',
            r'\d+[kkl]\s*ctc'
        ]
        
        for pattern in salary_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group()
        return ""
    
    def _extract_contact(self, text: str) -> str:
        """Extract contact information from text"""
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        phone_pattern = r'\+?\d{10,}'
        
        email_match = re.search(email_pattern, text)
        phone_match = re.search(phone_pattern, text)
        
        contact_info = []
        if email_match:
            contact_info.append(email_match.group())
        if phone_match:
            contact_info.append(phone_match.group())
        
        return ", ".join(contact_info)
    
    def _extract_website(self, text: str) -> str:
        """Extract website/application link from text"""
        url_pattern = r'https?://[^\s]+'
        match = re.search(url_pattern, text)
        return match.group() if match else ""
    
    def process_message(self, message_id: int, message_text: str) -> Dict[str, Any]:
        """Process a message through the ML pipeline"""
        try:
            # Classify the message
            classification = self.classify_message(message_text)
            
            # Score the message
            scores = self.score_message(message_text, classification.get('extracted_data', {}))
            
            # Store results in database
            job_score_data = {
                'message_id': message_id,
                **scores,
                'tags': self._generate_tags(scores, classification)
            }
            
            job_score_id = self.db.insert_job_score(job_score_data)
            
            # Store ML pipeline results
            ml_result_data = {
                'message_id': message_id,
                'classification': 'job_post' if classification['is_job_post'] else 'not_job',
                'confidence': classification.get('confidence', 0.0),
                'extracted_data': json.dumps(classification.get('extracted_data', {}))
            }
            
            return {
                'job_score_id': job_score_id,
                'scores': scores,
                'classification': classification,
                'tags': job_score_data['tags']
            }
            
        except Exception as e:
            logging.error(f"Error processing message {message_id}: {e}")
            return {}
    
    def _generate_tags(self, scores: Dict[str, float], classification: Dict[str, Any]) -> List[str]:
        """Generate tags based on scores and classification"""
        tags = []
        
        # Add tags based on scores
        if scores['remote_score'] >= 7.0:
            tags.append('remote')
        if scores['fresher_friendly_score'] >= 7.0:
            tags.append('fresher_friendly')
        if scores['overall_score'] >= 8.0:
            tags.append('high_quality')
        
        # Add tags based on extracted data
        extracted_data = classification.get('extracted_data', {})
        if extracted_data.get('skills'):
            tags.extend([f"skill_{skill}" for skill in extracted_data['skills'][:3]])
        if extracted_data.get('location'):
            tags.append(f"location_{extracted_data['location']}")
        
        return tags 