"""Génère un jeu de données synthétique d'offres d'emploi.
 - data/train_jobs.csv    : offres étiquetées (pour entraîner MLlib, Level 2)
 - data/jobs_stream.txt   : offres non étiquetées, une par ligne (pour le producer)
Pour un meilleur résultat, remplacez train_jobs.csv par un vrai dataset (colonnes: label,text).
"""
import csv
import random

DOMAINS = {
    "Data": {
        "titles": ["Data Engineer", "Data Scientist", "Data Analyst", "Ingénieur Big Data",
                   "Analyste BI", "Machine Learning Engineer", "Responsable Data", "Architecte Data"],
        "skills": ["SQL", "Spark", "Hadoop", "Kafka", "Airflow", "Power BI", "Tableau", "Python",
                   "pandas", "ETL", "data warehouse", "machine learning", "Hive", "Snowflake",
                   "statistiques", "modélisation prédictive", "data lake", "dbt"],
        "missions": ["construire des pipelines de données", "analyser de grands volumes de données",
                     "concevoir des tableaux de bord décisionnels", "entraîner et déployer des modèles prédictifs",
                     "alimenter le data lake et l'entrepôt de données"],
    },
    "Développement": {
        "titles": ["Développeur Full Stack", "Développeur Java", "Développeur Front-end React",
                   "Développeur Back-end Node.js", "Ingénieur logiciel", "Développeur PHP Symfony",
                   "Développeur mobile Flutter", "Développeur Angular"],
        "skills": ["Java", "Spring Boot", "React", "Angular", "Vue.js", "JavaScript", "TypeScript",
                   "Node.js", "PHP", "Symfony", "Laravel", "API REST", "microservices", "Git",
                   "tests unitaires", "HTML", "CSS", "Flutter"],
        "missions": ["développer de nouvelles fonctionnalités", "concevoir des API REST performantes",
                     "maintenir et refactorer le code existant", "intégrer des interfaces web responsives",
                     "participer aux revues de code et aux tests"],
    },
    "Sécurité": {
        "titles": ["Analyste SOC", "Ingénieur Cybersécurité", "Pentester", "Consultant Sécurité SI",
                   "RSSI", "Ingénieur sécurité réseau", "Auditeur sécurité"],
        "skills": ["SIEM", "pentest", "ISO 27001", "firewall", "détection d'intrusion",
                   "gestion des vulnérabilités", "cryptographie", "EDR", "OWASP", "Splunk",
                   "analyse de risques", "IAM", "forensique", "cybersécurité"],
        "missions": ["surveiller et traiter les incidents de sécurité", "réaliser des tests d'intrusion",
                     "définir la politique de sécurité du système d'information",
                     "auditer les applications et corriger les vulnérabilités",
                     "sensibiliser les équipes aux risques cyber"],
    },
    "Cloud": {
        "titles": ["Ingénieur Cloud", "DevOps Engineer", "Architecte Cloud AWS", "Administrateur Cloud Azure",
                   "Ingénieur SRE", "Cloud Engineer GCP", "Ingénieur Kubernetes"],
        "skills": ["AWS", "Azure", "GCP", "Kubernetes", "Docker", "Terraform", "Ansible", "CI/CD",
                   "Jenkins", "Helm", "Linux", "infrastructure as code", "Prometheus", "cloud"],
        "missions": ["automatiser le déploiement de l'infrastructure", "gérer les clusters Kubernetes en production",
                     "migrer les applications vers le cloud", "mettre en place les pipelines CI/CD",
                     "optimiser les coûts et la disponibilité des services cloud"],
    },
}
COMPANIES = ["TechNova", "DataWave", "Atlas Digital", "CloudiaSoft", "SecuriTech", "Orange Business",
             "CapSolutions", "Inwi", "Sopra Steria", "Devoteam", "OCP Digital", "Capgemini"]
CITIES = ["Rabat", "Casablanca", "Paris", "Lyon", "Tunis", "Montréal", "Bruxelles", "Tanger", "Remote"]
ALL_SKILLS = [s for d in DOMAINS.values() for s in d["skills"]]


def make_offer(domain, rng):
    d = DOMAINS[domain]
    title = rng.choice(d["titles"])
    skills = rng.sample(d["skills"], 3)
    if rng.random() < 0.15:                      # bruit : une compétence d'un autre domaine
        skills[rng.randrange(3)] = rng.choice(ALL_SKILLS)
    mission = rng.choice(d["missions"])
    company, city, exp = rng.choice(COMPANIES), rng.choice(CITIES), rng.randint(1, 8)
    if rng.random() < 0.8:
        return (f"{title} - {company}, {city}. Nous recherchons un(e) {title} maîtrisant "
                f"{skills[0]}, {skills[1]} et {skills[2]}. Mission : {mission}. CDI, {exp} ans d'expérience.")
    return (f"{company} is hiring a {title} ({city}). Required skills: {skills[0]}, {skills[1]}, "
            f"{skills[2]}. You will {mission}. {exp}+ years of experience.")


def main():
    rng = random.Random(42)
    with open("data/train_jobs.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["label", "text"])
        rows = [(dom, make_offer(dom, rng)) for dom in DOMAINS for _ in range(300)]
        rng.shuffle(rows)
        w.writerows(rows)

    rng = random.Random(7)
    with open("data/jobs_stream.txt", "w", encoding="utf-8") as f:
        for _ in range(80):
            f.write(make_offer(rng.choice(list(DOMAINS)), rng) + "\n")
    print("OK : data/train_jobs.csv (1200 lignes) et data/jobs_stream.txt (80 lignes)")


if __name__ == "__main__":
    main()
